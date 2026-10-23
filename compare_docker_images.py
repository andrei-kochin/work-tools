#!/usr/bin/env python3
"""Compare two container images and report the pull requests in the difference.

Image layers are cached locally. Pull-request lookup uses the GitHub CLI (`gh`)
and is cached under the same directory, so a later run can pass --offline.

    python compare_docker_images.py ^
        amdsiloai/vllm:qwen38-27b-211026 ^
        vllm/vllm-openai-rocm:v0.32.0 ^
        --aiter v0.1.26 ^
        --name1 silo --name2 upstream ^
        --out report.md

--aiter REF is the AITER release treated as installed on image 2. The vLLM
baseline is image 2's tag when that tag looks like v0.32.0, or --vllm-ref.
A file is "already in the baseline" when its bytes match that git ref, or when
they match a pull request that is merged into that ref. Everything else that
matches an open pull request is reported as unmerged. Bytes that match no
pull request are reported as unpublished.

Pass --files to also write the layer file listing.

Requires Python 3.11+. gh is required for the pull-request report.
docker is required only for --engine docker.
"""

from __future__ import annotations

import argparse
import difflib
import hashlib
import io
import json
import os
import re
import shutil
import subprocess
import sys
import tarfile
import textwrap
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass, field
from pathlib import Path

PIN_SUFFIXES = ("BRANCH", "REPO", "COMMIT", "VERSION", "IMAGE", "TAG")
PIN_RE = re.compile(r"\b([A-Z][A-Z0-9_]*)=([^\s\\]+)")
SITE_MARKERS = ("dist-packages/", "site-packages/")
COPY_RE = re.compile(r"\b(?:COPY|ADD)\b(.+?)(?:\s+# buildkit)?\s*$")

MANIFEST_ACCEPT = ", ".join(
    [
        "application/vnd.docker.distribution.manifest.v2+json",
        "application/vnd.docker.distribution.manifest.list.v2+json",
        "application/vnd.oci.image.manifest.v1+json",
        "application/vnd.oci.image.index.v1+json",
    ]
)


@dataclass
class ImageRef:
    raw: str
    registry: str
    repo: str
    tag: str
    digest: str | None

    @property
    def cache_key(self) -> str:
        digest = f"@{self.digest}" if self.digest else ""
        return f"{self.registry}/{self.repo}:{self.tag}{digest}"


@dataclass
class LayerInfo:
    index: int
    created_by: str
    empty: bool
    digest: str | None = None
    size: int | None = None
    skipped: str | None = None


@dataclass
class ImageInfo:
    ref: ImageRef
    name: str
    digest: str | None
    created: str | None
    env: dict[str, str]
    labels: dict[str, str]
    pins: dict[str, str]
    layers: list[LayerInfo]
    files: dict[str, "FileRec"] = field(default_factory=dict)
    engine: str = "registry"


@dataclass
class FileRec:
    path: str
    data: bytes
    norm_sha: str
    size: int
    layer_cmd: str


@dataclass
class Customize:
    target: int
    package: str
    source: Path
    ref: str | None
    strip_package: bool = False


def eprint(msg: str) -> None:
    print(msg, file=sys.stderr)


def parse_image_ref(text: str) -> ImageRef:
    raw = text.strip()
    digest = None
    body = raw
    if "@" in body:
        body, digest = body.split("@", 1)
    registry = "docker.io"
    parts = body.split("/")
    if parts and ("." in parts[0] or ":" in parts[0] or parts[0] == "localhost"):
        registry = parts[0]
        parts = parts[1:]
    if not parts or not parts[0]:
        raise SystemExit(f"not an image reference: {text}")
    tag = "latest"
    if ":" in parts[-1]:
        parts[-1], tag = parts[-1].rsplit(":", 1)
    repo = "/".join(parts) if len(parts) > 1 else f"library/{parts[0]}"
    return ImageRef(raw=raw, registry=registry, repo=repo, tag=tag, digest=digest)


def parse_customize(text: str) -> Customize:
    # TARGET:PACKAGE=SOURCE[@REF]
    try:
        target_s, rest = text.split(":", 1)
        package, source_ref = rest.split("=", 1)
    except ValueError as exc:
        raise SystemExit(
            "--customize must look like 2:aiter=C:\\src\\aiter@v0.1.26"
        ) from exc
    if target_s not in ("1", "2"):
        raise SystemExit("--customize target must be 1 or 2")
    ref = None
    source = source_ref
    # Drive letters are C:\... so only split @ref when @ is in the last path part
    # and is not an email-like middle segment. A trailing @ref is unambiguous.
    if "@" in Path(source_ref).name:
        source, ref = source_ref.rsplit("@", 1)
    if not package or not source:
        raise SystemExit(f"incomplete --customize value: {text}")
    return Customize(
        target=int(target_s),
        package=package.strip("/"),
        source=Path(source),
        ref=ref,
    )


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def normalize(data: bytes) -> bytes:
    if b"\0" in data:
        return data
    try:
        data.decode("utf-8")
    except UnicodeDecodeError:
        return data
    return data.replace(b"\r\n", b"\n")


def norm_sha(data: bytes) -> str:
    return sha256_bytes(normalize(data))


def site_rel(path: str) -> str | None:
    for marker in SITE_MARKERS:
        i = path.find(marker)
        if i >= 0:
            return path[i + len(marker) :]
    return None


def short_cmd(created_by: str, limit: int = 180) -> str:
    text = " ".join(created_by.split())
    for key in ("COPY ", "ADD ", "RUN "):
        i = text.rfind(key)
        if i >= 0:
            text = text[i:]
            break
    text = re.sub(r"\s+# buildkit$", "", text)
    if len(text) > limit:
        return text[: limit - 3] + "..."
    return text


def pins_from_history(layers: list[LayerInfo]) -> dict[str, str]:
    pins: dict[str, str] = {}
    for layer in layers:
        for match in PIN_RE.finditer(layer.created_by or ""):
            key, value = match.group(1), match.group(2).strip("\"'")
            if not (key.endswith(PIN_SUFFIXES) or key in {"BASE_IMAGE"}):
                continue
            # echo "NAME=${NAME}" is a shell reference, not the pin value.
            if value.startswith("$") or "${" in value:
                continue
            pins[key] = value
    return pins


def ignored_path(path: str, include_pyc: bool, include_app: bool) -> bool:
    if not include_pyc and ("/__pycache__/" in f"/{path}" or path.endswith(".pyc")):
        return True
    if not include_app and (
        path.startswith("app/vllm/benchmarks/")
        or path.startswith("app/vllm/examples/")
        or path.startswith("app/vllm/docker/")
    ):
        return True
    return False


def env_map(env_list: list[str] | None) -> dict[str, str]:
    out: dict[str, str] = {}
    for item in env_list or []:
        if "=" in item:
            k, v = item.split("=", 1)
            out[k] = v
    return out


class Cache:
    def __init__(self, root: Path, offline: bool) -> None:
        self.root = root
        self.offline = offline
        self.blob_dir = root / "blobs"
        self.index_path = root / "index.json"
        self.index: dict[str, dict] = {}
        if self.index_path.is_file():
            self.index = json.loads(self.index_path.read_text(encoding="utf-8"))

    def save(self) -> None:
        self.root.mkdir(parents=True, exist_ok=True)
        self.index_path.write_text(json.dumps(self.index, indent=2), encoding="utf-8")

    def blob_path(self, digest: str) -> Path:
        algo, hexpart = digest.split(":", 1)
        return self.blob_dir / algo / hexpart

    def has_blob(self, digest: str) -> bool:
        return self.blob_path(digest).is_file()

    def read_blob(self, digest: str) -> bytes | None:
        path = self.blob_path(digest)
        if path.is_file():
            return path.read_bytes()
        return None

    def write_blob(self, digest: str, data: bytes) -> None:
        path = self.blob_path(digest)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(data)


def _request(url: str, headers: dict[str, str]) -> tuple[bytes, dict[str, str]]:
    req = urllib.request.Request(url, headers=headers)
    with urllib.request.urlopen(req, timeout=120) as resp:
        return resp.read(), {k.lower(): v for k, v in resp.headers.items()}


def registry_token(registry: str, repo: str) -> str | None:
    if registry in {"docker.io", "registry-1.docker.io", "index.docker.io"}:
        qs = urllib.parse.urlencode(
            {
                "service": "registry.docker.io",
                "scope": f"repository:{repo}:pull",
            }
        )
        data, _ = _request(f"https://auth.docker.io/token?{qs}", {})
        return json.loads(data)["token"]
    url = f"https://{registry}/v2/"
    try:
        _request(url, {})
        return None
    except urllib.error.HTTPError as exc:
        if exc.code != 401:
            raise
        challenge = exc.headers.get("WWW-Authenticate", "")
    realm = scope = service = None
    for part in challenge.split(","):
        part = part.strip()
        if part.lower().startswith("bearer "):
            part = part[7:]
        if "=" not in part:
            continue
        k, v = part.split("=", 1)
        v = v.strip().strip('"')
        if k == "realm":
            realm = v
        elif k == "scope":
            scope = v
        elif k == "service":
            service = v
    if not realm:
        return None
    qs = urllib.parse.urlencode(
        {k: v for k, v in {"service": service, "scope": scope or f"repository:{repo}:pull"}.items() if v}
    )
    data, _ = _request(f"{realm}?{qs}", {})
    payload = json.loads(data)
    return payload.get("token") or payload.get("access_token")


def registry_host(registry: str) -> str:
    if registry in {"docker.io", "index.docker.io"}:
        return "registry-1.docker.io"
    return registry


def fetch_manifest(ref: ImageRef, cache: Cache) -> tuple[dict, str]:
    key = ref.cache_key
    cached = cache.index.get(key)
    if cached and cache.has_blob(cached["manifest_digest"]):
        eprint(f"cache manifest {ref.raw}")
        data = cache.read_blob(cached["manifest_digest"])
        assert data is not None
        return json.loads(data), cached["manifest_digest"]
    if cache.offline:
        raise SystemExit(
            f"offline: no cached manifest for {ref.raw}. Run once without --offline."
        )
    token = registry_token(ref.registry, ref.repo)
    ref_name = ref.digest or ref.tag
    url = f"https://{registry_host(ref.registry)}/v2/{ref.repo}/manifests/{ref_name}"
    headers = {"Accept": MANIFEST_ACCEPT}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    eprint(f"fetch manifest {ref.raw}")
    try:
        data, hdrs = _request(url, headers)
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", "replace")[:500]
        raise SystemExit(f"manifest {ref.raw} failed: HTTP {exc.code} {body}") from exc
    digest = hdrs.get("docker-content-digest") or "sha256:" + sha256_bytes(data)
    cache.write_blob(digest, data)
    cache.index[key] = {
        "registry": ref.registry,
        "repo": ref.repo,
        "tag": ref.tag,
        "digest": ref.digest,
        "manifest_digest": digest,
    }
    cache.save()
    return json.loads(data), digest


def select_manifest(doc: dict, ref: ImageRef, cache: Cache, platform: str) -> tuple[dict, str]:
    media = doc.get("mediaType", "")
    if "manifest.list" in media or "image.index" in media or "manifests" in doc and "layers" not in doc:
        os_name, arch = platform.split("/", 1) if "/" in platform else ("linux", platform)
        chosen = None
        for item in doc.get("manifests", []):
            plat = item.get("platform") or {}
            if plat.get("os") == os_name and plat.get("architecture") == arch:
                chosen = item
                break
        if chosen is None:
            raise SystemExit(f"no {platform} manifest in {ref.raw}")
        digest = chosen["digest"]
        if cache.has_blob(digest):
            data = cache.read_blob(digest)
            assert data is not None
            return json.loads(data), digest
        if cache.offline:
            raise SystemExit(f"offline: missing platform manifest {digest} for {ref.raw}")
        token = registry_token(ref.registry, ref.repo)
        url = f"https://{registry_host(ref.registry)}/v2/{ref.repo}/manifests/{digest}"
        headers = {"Accept": MANIFEST_ACCEPT}
        if token:
            headers["Authorization"] = f"Bearer {token}"
        eprint(f"fetch platform manifest {digest[:19]} {ref.raw}")
        data, hdrs = _request(url, headers)
        got = hdrs.get("docker-content-digest") or digest
        cache.write_blob(got, data)
        cache.save()
        return json.loads(data), got
    cached = cache.index.get(ref.cache_key, {})
    return doc, cached.get("manifest_digest", "sha256:" + sha256_bytes(json.dumps(doc).encode()))


def fetch_config(ref: ImageRef, manifest: dict, cache: Cache) -> dict:
    digest = manifest["config"]["digest"]
    cached = cache.read_blob(digest)
    if cached is not None:
        eprint(f"cache config {ref.raw}")
        return json.loads(cached)
    if cache.offline:
        raise SystemExit(f"offline: missing config blob {digest} for {ref.raw}")
    token = registry_token(ref.registry, ref.repo)
    url = f"https://{registry_host(ref.registry)}/v2/{ref.repo}/blobs/{digest}"
    headers = {}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    eprint(f"fetch config {ref.raw}")
    data, _ = _request(url, headers)
    cache.write_blob(digest, data)
    entry = cache.index.setdefault(ref.cache_key, {})
    entry["config_digest"] = digest
    cache.save()
    return json.loads(data)


def fetch_layer_blob(ref: ImageRef, digest: str, cache: Cache) -> bytes:
    cached = cache.read_blob(digest)
    if cached is not None:
        eprint(f"cache layer {digest[:19]}")
        return cached
    if cache.offline:
        raise SystemExit(f"offline: missing layer {digest}. Run once without --offline.")
    token = registry_token(ref.registry, ref.repo)
    url = f"https://{registry_host(ref.registry)}/v2/{ref.repo}/blobs/{digest}"
    headers = {}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    eprint(f"fetch layer {digest[:19]} ({ref.repo})")
    data, _ = _request(url, headers)
    cache.write_blob(digest, data)
    cache.save()
    return data


def align_layers(config: dict, manifest: dict) -> list[LayerInfo]:
    history = config.get("history") or []
    layer_docs = manifest.get("layers") or []
    cursor = 0
    out: list[LayerInfo] = []
    for i, item in enumerate(history):
        created_by = item.get("created_by") or item.get("comment") or ""
        if item.get("empty_layer"):
            out.append(LayerInfo(index=i, created_by=created_by, empty=True))
            continue
        if cursor >= len(layer_docs):
            out.append(
                LayerInfo(
                    index=i,
                    created_by=created_by,
                    empty=False,
                    skipped="history has no matching manifest layer",
                )
            )
            continue
        layer = layer_docs[cursor]
        cursor += 1
        out.append(
            LayerInfo(
                index=i,
                created_by=created_by,
                empty=False,
                digest=layer.get("digest"),
                size=int(layer.get("size") or 0),
            )
        )
    if cursor != len(layer_docs):
        eprint(
            f"warning: {len(layer_docs) - cursor} manifest layers were not paired with history"
        )
    return out


def files_in_layer(data: bytes) -> tuple[dict[str, bytes], list[str]]:
    files: dict[str, bytes] = {}
    deleted: list[str] = []
    with tarfile.open(fileobj=io.BytesIO(data), mode="r:*") as tf:
        for member in tf:
            name = member.name.lstrip("./")
            if not name:
                continue
            base = name.rsplit("/", 1)[-1]
            if base.startswith(".wh."):
                parent = name.rsplit("/", 1)[0] if "/" in name else ""
                target = f"{parent}/{base[4:]}" if parent else base[4:]
                target = target.lstrip("/")
                files.pop(target, None)
                deleted.append(target)
                continue
            if not member.isfile():
                continue
            extracted = tf.extractfile(member)
            if extracted is None:
                continue
            files[name] = extracted.read()
    return files, deleted


def load_registry_image(
    ref: ImageRef,
    name: str,
    cache: Cache,
    platform: str,
    tail_layers: int,
    max_layer_bytes: int,
) -> ImageInfo:
    manifest_doc, manifest_digest = fetch_manifest(ref, cache)
    manifest, manifest_digest = select_manifest(manifest_doc, ref, cache, platform)
    config = fetch_config(ref, manifest, cache)
    layers = align_layers(config, manifest)
    cfg = config.get("config") or {}
    info = ImageInfo(
        ref=ref,
        name=name,
        digest=manifest_digest,
        created=config.get("created"),
        env=env_map(cfg.get("Env")),
        labels={k: str(v) for k, v in (cfg.get("Labels") or {}).items() if v is not None},
        pins=pins_from_history(layers),
        layers=layers,
        engine="registry",
    )
    nonempty = [layer for layer in layers if not layer.empty and layer.digest]
    tail = nonempty[-tail_layers:] if tail_layers else nonempty
    files: dict[str, FileRec] = {}
    for layer in tail:
        size = layer.size or 0
        if size > max_layer_bytes:
            layer.skipped = f"larger than {max_layer_bytes} bytes"
            continue
        assert layer.digest is not None
        blob = fetch_layer_blob(ref, layer.digest, cache)
        layer_files, _deleted = files_in_layer(blob)
        for path, data in layer_files.items():
            files[path] = FileRec(
                path=path,
                data=data,
                norm_sha=norm_sha(data),
                size=len(data),
                layer_cmd=short_cmd(layer.created_by),
            )
    info.files = files
    return info


def docker_available() -> bool:
    return shutil.which("docker") is not None


def docker_inspect(ref: str) -> dict | None:
    if not docker_available():
        return None
    proc = subprocess.run(
        ["docker", "image", "inspect", ref],
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
    )
    if proc.returncode != 0:
        return None
    data = json.loads(proc.stdout)
    return data[0] if data else None


def parse_human_size(text: str) -> int:
    text = text.strip()
    match = re.fullmatch(r"([0-9]*\.?[0-9]+)\s*([A-Za-z]+)", text)
    if not match:
        if text in {"0", "0B"}:
            return 0
        return 0
    value = float(match.group(1))
    unit = match.group(2)
    table = {
        "B": 1,
        "kB": 1000,
        "KB": 1000,
        "MB": 1000**2,
        "GB": 1000**3,
        "KiB": 1024,
        "MiB": 1024**2,
        "GiB": 1024**3,
    }
    return int(value * table.get(unit, 1))


def copy_destinations(created_by: str) -> list[str]:
    text = " ".join(created_by.split())
    match = COPY_RE.search(text)
    if not match:
        return []
    body = match.group(1).strip()
    tokens = [tok for tok in body.split() if not tok.startswith("--")]
    # COPY supports flags glued as --chown=... already excluded by startswith --
    if len(tokens) < 2:
        return []
    dest = tokens[-1]
    if not dest.startswith("/"):
        return []
    return [dest]


def docker_cp_tree(image: str, paths: list[str]) -> dict[str, bytes]:
    proc = subprocess.run(
        ["docker", "create", "--entrypoint", "", image, "true"],
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
    )
    if proc.returncode != 0:
        raise SystemExit(f"docker create {image} failed: {proc.stderr.strip()}")
    cid = proc.stdout.strip()
    files: dict[str, bytes] = {}
    tmp = Path(os.environ.get("TEMP", "/tmp")) / f"imgcmp-{cid[:12]}"
    tmp.mkdir(parents=True, exist_ok=True)
    try:
        for dest in paths:
            host = tmp / dest.lstrip("/").replace("/", os.sep)
            if host.exists():
                if host.is_dir():
                    shutil.rmtree(host)
                else:
                    host.unlink()
            host.parent.mkdir(parents=True, exist_ok=True)
            cp = subprocess.run(
                ["docker", "cp", f"{cid}:{dest}", str(host)],
                capture_output=True,
                text=True,
                encoding="utf-8",
                errors="replace",
            )
            if cp.returncode != 0:
                continue
            if host.is_file():
                data = host.read_bytes()
                files[dest.lstrip("/")] = data
            elif host.is_dir():
                for file in host.rglob("*"):
                    if file.is_file():
                        rel = file.relative_to(tmp).as_posix()
                        files[rel] = file.read_bytes()
    finally:
        subprocess.run(["docker", "rm", cid], capture_output=True)
        shutil.rmtree(tmp, ignore_errors=True)
    return files


def load_docker_image(
    ref: ImageRef,
    name: str,
    tail_layers: int,
    max_layer_bytes: int,
) -> ImageInfo:
    inspected = docker_inspect(ref.raw)
    if inspected is None:
        raise SystemExit(f"docker does not have a local image named {ref.raw}")
    proc = subprocess.run(
        ["docker", "history", "--no-trunc", "--format", "{{json .}}", ref.raw],
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
    )
    if proc.returncode != 0:
        raise SystemExit(proc.stderr.strip() or f"docker history failed for {ref.raw}")
    # docker history is newest-first; config history is oldest-first.
    rows = [json.loads(line) for line in proc.stdout.splitlines() if line.strip()]
    rows.reverse()
    layers: list[LayerInfo] = []
    for i, row in enumerate(rows):
        created_by = row.get("CreatedBy") or ""
        size = parse_human_size(str(row.get("Size") or "0B"))
        empty = size == 0 and not created_by.startswith(("COPY", "ADD"))
        layers.append(
            LayerInfo(
                index=i,
                created_by=created_by,
                empty=empty,
                size=size,
            )
        )
    cfg = inspected.get("Config") or {}
    info = ImageInfo(
        ref=ref,
        name=name,
        digest=(inspected.get("RepoDigests") or [None])[0],
        created=inspected.get("Created"),
        env=env_map(cfg.get("Env")),
        labels={k: str(v) for k, v in (cfg.get("Labels") or {}).items() if v is not None},
        pins=pins_from_history(layers),
        layers=layers,
        engine="docker",
    )
    nonempty = [layer for layer in layers if not layer.empty]
    tail = nonempty[-tail_layers:] if tail_layers else nonempty
    wanted: list[str] = []
    for layer in tail:
        size = layer.size or 0
        if size > max_layer_bytes and not copy_destinations(layer.created_by):
            layer.skipped = f"larger than {max_layer_bytes} bytes"
            continue
        dests = copy_destinations(layer.created_by)
        if not dests and size > max_layer_bytes:
            layer.skipped = f"larger than {max_layer_bytes} bytes"
            continue
        wanted.extend(dests)
    eprint(f"docker cp {len(wanted)} paths from {ref.raw}")
    raw_files = docker_cp_tree(ref.raw, wanted) if wanted else {}
    files: dict[str, FileRec] = {}
    cmd_for_path: dict[str, str] = {}
    for layer in tail:
        for dest in copy_destinations(layer.created_by):
            cmd_for_path[dest.lstrip("/")] = short_cmd(layer.created_by)
    for path, data in raw_files.items():
        files[path] = FileRec(
            path=path,
            data=data,
            norm_sha=norm_sha(data),
            size=len(data),
            layer_cmd=cmd_for_path.get(path, "docker cp"),
        )
    info.files = files
    return info


def load_image(
    ref: ImageRef,
    name: str,
    cache: Cache,
    engine: str,
    platform: str,
    tail_layers: int,
    max_layer_bytes: int,
) -> ImageInfo:
    use_docker = engine == "docker" or (
        engine == "auto" and cache.offline and docker_inspect(ref.raw) is not None
    )
    if engine == "auto" and not cache.offline:
        use_docker = False
    if use_docker:
        return load_docker_image(ref, name, tail_layers, max_layer_bytes)
    return load_registry_image(ref, name, cache, platform, tail_layers, max_layer_bytes)


def prepare_customize(cust: Customize) -> None:
    if not cust.source.exists():
        raise SystemExit(f"customization source does not exist: {cust.source}")
    if cust.ref:
        proc = subprocess.run(
            ["git", "-C", str(cust.source), "rev-parse", "--verify", cust.ref],
            capture_output=True,
            text=True,
        )
        if proc.returncode != 0:
            raise SystemExit(
                f"git ref {cust.ref} not found in {cust.source}. "
                "Fetch the tag locally first; --offline will not clone."
            )
        listing = subprocess.run(
            ["git", "-C", str(cust.source), "ls-tree", "--name-only", cust.ref],
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
        )
        names = {line.strip() for line in listing.stdout.splitlines() if line.strip()}
    else:
        names = {p.name for p in cust.source.iterdir()}
    cust.strip_package = cust.package not in names


def customization_bytes(cust: Customize, rel: str) -> bytes | None:
    git_path = rel
    prefix = cust.package + "/"
    if cust.strip_package:
        if rel == cust.package:
            return None
        if not rel.startswith(prefix):
            return None
        git_path = rel[len(prefix) :]
    elif rel != cust.package and not rel.startswith(prefix):
        return None
    if cust.ref:
        proc = subprocess.run(
            ["git", "-C", str(cust.source), "show", f"{cust.ref}:{git_path}"],
            capture_output=True,
        )
        if proc.returncode != 0:
            return None
        return proc.stdout
    path = cust.source / git_path
    if path.is_file():
        return path.read_bytes()
    return None


def text_diff(left: bytes, right: bytes, left_name: str, right_name: str, limit: int) -> str:
    try:
        a = normalize(left).decode("utf-8").splitlines()
        b = normalize(right).decode("utf-8").splitlines()
    except UnicodeDecodeError:
        return "(binary file)\n"
    lines = list(
        difflib.unified_diff(a, b, fromfile=left_name, tofile=right_name, lineterm="", n=2)
    )
    if len(lines) > limit:
        lines = lines[:limit] + [f"... {len(lines) - limit} more diff lines"]
    return "\n".join(lines) + ("\n" if lines else "")


def diff_maps(
    left: dict[str, str],
    right: dict[str, str],
) -> tuple[list[str], list[str], list[str]]:
    only_left = sorted(set(left) - set(right))
    only_right = sorted(set(right) - set(left))
    changed = sorted(k for k in set(left) & set(right) if left[k] != right[k])
    return only_left, only_right, changed


def file_rows(info: ImageInfo) -> dict[str, FileRec]:
    return info.files


def display_path(path: str) -> str:
    rel = site_rel(path)
    return rel or path


def render(
    image1: ImageInfo,
    image2: ImageInfo,
    customizations: list[Customize],
    max_diff_lines: int,
    include_pyc: bool = False,
    include_app: bool = False,
) -> tuple[str, dict]:
    name1, name2 = image1.name, image2.name
    env_only1, env_only2, env_changed = diff_maps(image1.env, image2.env)
    pin_only1, pin_only2, pin_changed = diff_maps(image1.pins, image2.pins)

    def layer_lines(info: ImageInfo) -> list[str]:
        lines = []
        for layer in info.layers:
            if layer.empty:
                continue
            size = layer.size if layer.size is not None else "?"
            flag = f" SKIP {layer.skipped}" if layer.skipped else ""
            lines.append(f"{size:>12}  {short_cmd(layer.created_by)}{flag}")
        return lines

    cmds1 = {short_cmd(layer.created_by) for layer in image1.layers if not layer.empty}
    cmds2 = {short_cmd(layer.created_by) for layer in image2.layers if not layer.empty}

    files1 = {
        path: rec
        for path, rec in file_rows(image1).items()
        if not ignored_path(path, include_pyc, include_app)
    }
    files2 = {
        path: rec
        for path, rec in file_rows(image2).items()
        if not ignored_path(path, include_pyc, include_app)
    }
    by_sha1 = {rec.path: rec.norm_sha for rec in files1.values()}
    by_sha2 = {rec.path: rec.norm_sha for rec in files2.values()}
    only1, only2, changed = diff_maps(by_sha1, by_sha2)

    report: dict = {
        "image1": {
            "name": name1,
            "ref": image1.ref.raw,
            "digest": image1.digest,
            "created": image1.created,
            "engine": image1.engine,
            "pins": image1.pins,
        },
        "image2": {
            "name": name2,
            "ref": image2.ref.raw,
            "digest": image2.digest,
            "created": image2.created,
            "engine": image2.engine,
            "pins": image2.pins,
        },
        "env": {"only1": env_only1, "only2": env_only2, "changed": env_changed},
        "pins": {"only1": pin_only1, "only2": pin_only2, "changed": pin_changed},
        "files": {
            "only1": [display_path(p) for p in only1],
            "only2": [display_path(p) for p in only2],
            "changed": [display_path(p) for p in changed],
            "scanned1": len(files1),
            "scanned2": len(files2),
        },
        "customizations": [],
    }

    lines: list[str] = []
    lines.append(f"# {name1} vs {name2}")
    lines.append("")
    lines.append("Scanned layers are the tail of each image, size-capped. A path that does not appear was inside a skipped layer or was not copied in the tail. It is not proof the file is absent from the image.")
    lines.append("")
    lines.append("## Images")
    lines.append("")
    lines.append("| | " + name1 + " | " + name2 + " |")
    lines.append("|---|---|---|")
    lines.append(f"| ref | `{image1.ref.raw}` | `{image2.ref.raw}` |")
    lines.append(f"| digest | `{image1.digest}` | `{image2.digest}` |")
    lines.append(f"| created | {image1.created} | {image2.created} |")
    lines.append(f"| engine | {image1.engine} | {image2.engine} |")
    lines.append(f"| scanned files | {len(files1)} | {len(files2)} |")
    lines.append("")

    lines.append("## Version pins from image history")
    lines.append("")
    pin_keys = sorted(set(image1.pins) | set(image2.pins))
    if not pin_keys:
        lines.append("No BRANCH/REPO/COMMIT/VERSION pins were found in history.")
    else:
        lines.append("| pin | " + name1 + " | " + name2 + " |")
        lines.append("|---|---|---|")
        for key in pin_keys:
            lines.append(
                f"| `{key}` | `{image1.pins.get(key, '')}` | `{image2.pins.get(key, '')}` |"
            )
    lines.append("")

    lines.append("## Environment")
    lines.append("")
    if not env_only1 and not env_only2 and not env_changed:
        lines.append("Env keys match.")
    else:
        if env_changed:
            lines.append("Changed:")
            lines.append("")
            for key in env_changed:
                lines.append(f"- `{key}`")
                lines.append(f"  - {name1}: `{image1.env[key]}`")
                lines.append(f"  - {name2}: `{image2.env[key]}`")
        if env_only1:
            lines.append("")
            lines.append(f"Only in {name1}: " + ", ".join(f"`{k}`" for k in env_only1))
        if env_only2:
            lines.append("")
            lines.append(f"Only in {name2}: " + ", ".join(f"`{k}`" for k in env_only2))
    lines.append("")

    def emit_cmds(title: str, cmds: list[str]) -> None:
        lines.append(f"### {title}")
        lines.append("")
        if not cmds:
            lines.append("(none)")
        for cmd in cmds:
            lines.append(f"- `{cmd}`")
        lines.append("")

    only_cmd1 = sorted(cmds1 - cmds2)
    only_cmd2 = sorted(cmds2 - cmds1)
    lines.append("## History commands only on one side")
    lines.append("")
    emit_cmds(
        f"COPY/ADD only in {name1}",
        [cmd for cmd in only_cmd1 if cmd.startswith(("COPY ", "ADD "))],
    )
    emit_cmds(
        f"COPY/ADD only in {name2}",
        [cmd for cmd in only_cmd2 if cmd.startswith(("COPY ", "ADD "))],
    )
    emit_cmds(
        f"Other commands only in {name1}",
        [cmd for cmd in only_cmd1 if not cmd.startswith(("COPY ", "ADD "))],
    )
    emit_cmds(
        f"Other commands only in {name2}",
        [cmd for cmd in only_cmd2 if not cmd.startswith(("COPY ", "ADD "))],
    )

    lines.append("## Skipped layers")
    lines.append("")
    for info in (image1, image2):
        skipped = [layer for layer in info.layers if layer.skipped]
        lines.append(f"### {info.name} ({len(skipped)})")
        lines.append("")
        for layer in skipped[-30:]:
            size = layer.size if layer.size is not None else "?"
            lines.append(f"- {size} bytes: `{short_cmd(layer.created_by)}` — {layer.skipped}")
        if len(skipped) > 30:
            lines.append(f"- ... {len(skipped) - 30} earlier skipped layers")
        if not skipped:
            lines.append("(none)")
        lines.append("")

    def emit_files(title: str, paths: list[str], owner: ImageInfo | None) -> None:
        lines.append(f"### {title} ({len(paths)})")
        lines.append("")
        if not paths:
            lines.append("(none)")
            lines.append("")
            return
        shown = paths[:200]
        for path in shown:
            rec = owner.files[path] if owner and path in owner.files else None
            extra = f" — {rec.layer_cmd}" if rec else ""
            lines.append(f"- `{display_path(path)}`{extra}")
        if len(paths) > len(shown):
            lines.append(f"- ... {len(paths) - len(shown)} more in the JSON report")
        lines.append("")

    lines.append("## File delta in scanned layers")
    lines.append("")
    lines.append(
        f"{len(only1)} only in {name1}, {len(only2)} only in {name2}, {len(changed)} changed."
    )
    lines.append("")
    emit_files(f"Only in {name1}", only1, image1)
    emit_files(f"Only in {name2}", only2, image2)
    emit_files("Content differs", changed, image1)
    for path in changed[:20]:
        lines.append(f"#### `{display_path(path)}`")
        lines.append("")
        lines.append("```diff")
        lines.append(
            text_diff(
                files2[path].data,
                files1[path].data,
                name2,
                name1,
                max_diff_lines,
            ).rstrip()
        )
        lines.append("```")
        lines.append("")

    for cust in customizations:
        base = image1 if cust.target == 1 else image2
        other = image2 if cust.target == 1 else image1
        # Files of interest: the other image's scanned files under this package,
        # plus the base image's scanned files under this package.
        interesting: list[str] = []
        for info in (image1, image2):
            for path in info.files:
                if ignored_path(path, include_pyc, include_app):
                    continue
                rel = site_rel(path) or path
                if rel == cust.package or rel.startswith(cust.package + "/"):
                    interesting.append(path)
        interesting = sorted(set(interesting))
        matched, differed, missing = [], [], []
        diffs: list[tuple[str, str]] = []
        for path in interesting:
            rel = site_rel(path) or path
            blob = customization_bytes(cust, rel)
            # Prefer the image that is NOT the customization target as the thing
            # being explained. If both have the file, explain `other` (the image
            # the customization was not applied to) against the tree, and also
            # note the target image when it differs.
            subject = other.files.get(path) or base.files.get(path)
            if subject is None:
                continue
            label = display_path(path)
            if blob is None:
                missing.append(label)
                continue
            if norm_sha(blob) == subject.norm_sha:
                matched.append(label)
            else:
                differed.append(label)
                diffs.append(
                    (
                        label,
                        text_diff(
                            blob,
                            subject.data,
                            f"{cust.package}@{cust.ref or 'worktree'}",
                            subject.layer_cmd or other.name,
                            max_diff_lines,
                        ),
                    )
                )
        ref_label = cust.ref or "worktree"
        title = f"{cust.package} @ {ref_label} on image {cust.target} ({base.name})"
        lines.append(f"## Customization: {title}")
        lines.append("")
        lines.append(f"Source: `{cust.source}`")
        lines.append("")
        lines.append(
            f"Compared scanned `{cust.package}` files with that tree. "
            f"{len(matched)} match, {len(differed)} differ, {len(missing)} are not in the tree."
        )
        lines.append("")
        lines.append(f"### Match {cust.package}@{ref_label}")
        lines.append("")
        for item in matched:
            lines.append(f"- `{item}`")
        if not matched:
            lines.append("(none)")
        lines.append("")
        lines.append(f"### Differ from {cust.package}@{ref_label}")
        lines.append("")
        for item in differed:
            lines.append(f"- `{item}`")
        if not differed:
            lines.append("(none)")
        lines.append("")
        lines.append("### Not in the customization tree")
        lines.append("")
        for item in missing:
            lines.append(f"- `{item}`")
        if not missing:
            lines.append("(none)")
        lines.append("")
        for label, diff in diffs[:15]:
            lines.append(f"#### `{label}`")
            lines.append("")
            lines.append("```diff")
            lines.append(diff.rstrip())
            lines.append("```")
            lines.append("")
        report["customizations"].append(
            {
                "target": cust.target,
                "package": cust.package,
                "source": str(cust.source),
                "ref": cust.ref,
                "match": matched,
                "differ": differed,
                "missing": missing,
            }
        )

    lines.append("## Tail history")
    lines.append("")
    for info, cmds in ((image1, layer_lines(image1)), (image2, layer_lines(image2))):
        lines.append(f"### {info.name}")
        lines.append("")
        lines.append("```text")
        tail = cmds[-40:]
        if len(cmds) > 40:
            lines.append(f"... {len(cmds) - 40} earlier non-empty layers")
        lines.extend(tail)
        lines.append("```")
        lines.append("")

    return "\n".join(lines).rstrip() + "\n", report


def _rmtree(path: Path) -> None:
    def _onexc(func, target, _exc):
        os.chmod(target, 0o666)
        func(target)

    if path.exists():
        shutil.rmtree(path, onexc=_onexc)


def self_test() -> None:
    tmp = Path(os.environ.get("TEMP", "/tmp")) / "docker-image-compare-selftest"
    _rmtree(tmp)
    repo = tmp / "aiter"
    (repo / "aiter" / "ops").mkdir(parents=True)
    (repo / "aiter" / "ops" / "a.py").write_text("release\n", encoding="utf-8")
    (repo / "aiter" / "ops" / "b.py").write_text("same\n", encoding="utf-8")
    subprocess.check_call(["git", "init"], cwd=repo, stdout=subprocess.DEVNULL)
    subprocess.check_call(["git", "add", "."], cwd=repo, stdout=subprocess.DEVNULL)
    subprocess.check_call(
        ["git", "-c", "user.email=test@example.com", "-c", "user.name=test", "commit", "-m", "init"],
        cwd=repo,
        stdout=subprocess.DEVNULL,
    )
    cust = Customize(target=2, package="aiter", source=repo, ref="HEAD")
    prepare_customize(cust)
    assert cust.strip_package is False
    assert customization_bytes(cust, "aiter/ops/a.py") == b"release\n"
    assert customization_bytes(cust, "aiter/ops/missing.py") is None

    def rec(path: str, text: str, cmd: str) -> FileRec:
        data = text.encode()
        return FileRec(path=path, data=data, norm_sha=norm_sha(data), size=len(data), layer_cmd=cmd)

    ref = parse_image_ref("example/one:latest")
    image1 = ImageInfo(
        ref=ref,
        name="silo",
        digest="sha256:abc",
        created="2026-01-01",
        env={"A": "1", "B": "x"},
        labels={},
        pins={"AITER_BRANCH": "v0.1.22.post1"},
        layers=[],
        engine="test",
        files={
            "usr/local/lib/python3.12/dist-packages/aiter/ops/a.py": rec(
                "usr/local/lib/python3.12/dist-packages/aiter/ops/a.py", "patched\n", "COPY a.py"
            ),
            "usr/local/lib/python3.12/dist-packages/aiter/ops/b.py": rec(
                "usr/local/lib/python3.12/dist-packages/aiter/ops/b.py", "same\n", "COPY b.py"
            ),
            "usr/local/lib/python3.12/dist-packages/vllm/envs.py": rec(
                "usr/local/lib/python3.12/dist-packages/vllm/envs.py", "mono\n", "COPY envs.py"
            ),
        },
    )
    image2 = ImageInfo(
        ref=parse_image_ref("example/two:v0"),
        name="upstream",
        digest="sha256:def",
        created="2026-01-02",
        env={"A": "1", "B": "y"},
        labels={},
        pins={"AITER_BRANCH": "v0.1.26"},
        layers=[],
        engine="test",
        files={
            "usr/local/lib/python3.12/dist-packages/vllm/envs.py": rec(
                "usr/local/lib/python3.12/dist-packages/vllm/envs.py", "stock\n", "wheel"
            ),
        },
    )
    text, payload = render(image1, image2, [cust], 40)
    cust_payload = payload["customizations"][0]
    assert "aiter/ops/b.py" in cust_payload["match"], cust_payload
    assert "aiter/ops/a.py" in cust_payload["differ"], cust_payload
    assert "vllm/envs.py" in payload["files"]["changed"]
    assert "AITER_BRANCH" in text
    assert "patched" in text
    _rmtree(tmp)
    mentions = pull_mentions(
        "Adapted from SGLang #43558. See https://github.com/ROCm/aiter/pull/4970."
    )
    assert ("sgl-project/sglang", 43558) in mentions
    assert ("ROCm/aiter", 4970) in mentions
    assert repo_for_rel("aiter/ops/flydsl/decode_gemm_mxfp4.py") == (
        "ROCm/aiter",
        "aiter/ops/flydsl/decode_gemm_mxfp4.py",
    )
    eprint("self-test ok")


PACKAGE_REPOS = {
    "aiter": "ROCm/aiter",
    "vllm": "vllm-project/vllm",
}
NAME_REPOS = {
    "aiter": "ROCm/aiter",
    "vllm": "vllm-project/vllm",
    "sglang": "sgl-project/sglang",
    "flydsl": "ROCm/FlyDSL",
}
LOW_VALUE_NAMES = {
    "__init__.py",
    "utils.py",
    "envs.py",
    "arg_utils.py",
    "kernels_common.py",
    "common.py",
}
MENTION_URL_RE = re.compile(
    r"https://github\.com/([\w.-]+/[\w.-]+)/(?:pull|issues)/(\d+)"
)
MENTION_NAME_RE = re.compile(
    r"\b(aiter|vllm|sglang|flydsl)\s+#(\d+)",
    re.IGNORECASE,
)
MENTION_REPO_RE = re.compile(r"\b([\w.-]+/[\w.-]+)#(\d+)")
TOKEN_RE = re.compile(r"\b((?:VLLM|AITER|SGLANG)_[A-Z0-9_]{10,})\b")
COMMON_TOKENS = {
    "VLLM_ROCM_USE_AITER",
    "VLLM_ROCM_USE_AITER_LINEAR",
    "VLLM_ROCM_USE_AITER_RMSNORM",
    "VLLM_ROCM_USE_AITER_MHA",
    "VLLM_ENABLE_V1_MULTIPROCESSING",
}


def repo_for_rel(rel: str) -> tuple[str, str] | None:
    for package, repo in PACKAGE_REPOS.items():
        if rel == package or rel.startswith(package + "/"):
            return repo, rel
    return None


def pull_mentions(text: str) -> list[tuple[str, int]]:
    found: list[tuple[str, int]] = []
    seen: set[tuple[str, int]] = set()

    def add(repo: str, number: int) -> None:
        key = (repo, number)
        if key in seen or number <= 0:
            return
        seen.add(key)
        found.append(key)

    for repo, number in MENTION_URL_RE.findall(text):
        add(repo, int(number))
    for name, number in MENTION_NAME_RE.findall(text):
        repo = NAME_REPOS.get(name.lower())
        if repo:
            add(repo, int(number))
    for repo, number in MENTION_REPO_RE.findall(text):
        if "/" in repo and not repo.startswith("application/"):
            add(repo, int(number))
    return found


def rare_tokens(data: bytes) -> list[str]:
    try:
        text = data.decode("utf-8")
    except UnicodeDecodeError:
        return []
    counts: dict[str, int] = {}
    for token in TOKEN_RE.findall(text):
        if token in COMMON_TOKENS:
            continue
        counts[token] = counts.get(token, 0) + 1
    ranked = sorted(counts, key=lambda item: (counts[item], -len(item)))
    return ranked[:4]


def _gh(args: list[str], binary: bool = False) -> subprocess.CompletedProcess:
    def once(env: dict[str, str]) -> subprocess.CompletedProcess:
        return subprocess.run(
            ["gh", *args],
            capture_output=True,
            env=env,
            check=False,
        )

    env = os.environ.copy()
    env["GH_PAGER"] = ""
    proc = once(env)
    err = proc.stderr.decode("utf-8", "replace")
    if proc.returncode != 0 and "Bad credentials" in err:
        env.pop("GITHUB_TOKEN", None)
        env.pop("GH_TOKEN", None)
        proc = once(env)
    return proc


class GhCache:
    def __init__(self, root: Path, offline: bool) -> None:
        self.root = root / "gh"
        self.offline = offline
        self.root.mkdir(parents=True, exist_ok=True)

    def _path(self, key: str) -> Path:
        digest = hashlib.sha256(key.encode()).hexdigest()
        return self.root / digest[:2] / digest

    def get_bytes(self, key: str) -> bytes | None:
        path = self._path(key)
        if not path.is_file():
            return None
        return path.read_bytes()

    def put_bytes(self, key: str, data: bytes) -> None:
        path = self._path(key)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(data)

    def json(self, key: str, args: list[str]):
        cached = self.get_bytes(key)
        if cached == b"\0":
            return None
        if cached is not None:
            return json.loads(cached.decode("utf-8"))
        if self.offline:
            raise SystemExit(
                "offline: missing a cached GitHub response. Run once without --offline."
            )
        proc = _gh(args)
        if proc.returncode != 0:
            err = proc.stderr.decode("utf-8", "replace")
            if "HTTP 404" in err or "Not Found" in err:
                self.put_bytes(key, b"\0")
                return None
            raise SystemExit(f"gh {' '.join(args[:6])} failed: {err.strip()[:400]}")
        self.put_bytes(key, proc.stdout)
        return json.loads(proc.stdout.decode("utf-8"))

    def raw(self, key: str, args: list[str]) -> bytes | None:
        cached = self.get_bytes(key)
        if cached == b"\0":
            return None
        if cached is not None:
            return cached
        if self.offline:
            raise SystemExit(
                "offline: missing a cached GitHub file. Run once without --offline."
            )
        proc = _gh(args, binary=True)
        if proc.returncode != 0:
            err = proc.stderr.decode("utf-8", "replace")
            if "HTTP 404" in err or "Not Found" in err:
                self.put_bytes(key, b"\0")
                return None
            raise SystemExit(f"gh {' '.join(args[:6])} failed: {err.strip()[:400]}")
        # A JSON error body can still come back with exit 0 from some wrappers.
        if proc.stdout.startswith(b"{") and b'"message"' in proc.stdout[:200]:
            self.put_bytes(key, b"\0")
            return None
        self.put_bytes(key, proc.stdout)
        return proc.stdout


def file_at_ref(gh: GhCache, repo: str, path: str, ref: str) -> bytes | None:
    quoted = urllib.parse.quote(ref, safe="")
    encoded_path = "/".join(urllib.parse.quote(part) for part in path.split("/"))
    return gh.raw(
        f"raw {repo} {ref} {path}",
        [
            "api",
            "-H",
            "Accept: application/vnd.github.raw",
            f"repos/{repo}/contents/{encoded_path}?ref={quoted}",
        ],
    )


def search_prs(gh: GhCache, repo: str, query: str) -> list[dict]:
    data = gh.json(
        f"search {repo} {query}",
        [
            "search",
            "prs",
            "--repo",
            repo,
            "--limit",
            "6",
            "--json",
            "number,title,state,url,isDraft",
            "--",
            query,
        ],
    )
    return data or []


def pr_info(gh: GhCache, repo: str, number: int) -> dict | None:
    return gh.json(
        f"pr {repo} {number}",
        [
            "pr",
            "view",
            str(number),
            "--repo",
            repo,
            "--json",
            "number,title,state,mergedAt,url,isDraft,body,files,mergeCommit",
        ],
    )


def merged_into(gh: GhCache, repo: str, ref: str, sha: str) -> bool | None:
    """True when sha is an ancestor of ref (or the same commit).

    ``gh api --jq .status`` prints a bare word (``behind``), not JSON.
    A cached copy of that word must be read as text.
    """
    if not sha:
        return None
    key = f"compare {repo} {ref} {sha}"
    cached = gh.get_bytes(key)
    if cached == b"\0":
        return None
    if cached is None:
        if gh.offline:
            raise SystemExit(
                "offline: missing a cached GitHub response. Run once without --offline."
            )
        proc = _gh(["api", f"repos/{repo}/compare/{ref}...{sha}", "--jq", ".status"])
        if proc.returncode != 0:
            err = proc.stderr.decode("utf-8", "replace")
            if any(token in err for token in ("HTTP 404", "Not Found", "HTTP 406")):
                gh.put_bytes(key, b"\0")
                return None
            raise SystemExit(f"gh compare {repo} failed: {err.strip()[:400]}")
        payload = proc.stdout.strip() or b"\0"
        gh.put_bytes(key, payload)
        cached = payload
    text = cached.decode("utf-8", "replace").strip().strip('"')
    if text.startswith("{"):
        try:
            data = json.loads(text)
        except json.JSONDecodeError:
            return None
        text = str((data or {}).get("status") or "")
    return text in {"behind", "identical"}


def commits_touching(gh: GhCache, repo: str, ref: str, path: str) -> list[str]:
    encoded = urllib.parse.quote(path, safe="/")
    data = gh.json(
        f"commits {repo} {ref} {path}",
        [
            "api",
            f"repos/{repo}/commits?path={encoded}&sha={urllib.parse.quote(ref, safe='')}&per_page=8",
            "--jq",
            "[.[].sha]",
        ],
    )
    return data or []


def pulls_for_commit(gh: GhCache, repo: str, sha: str) -> list[dict]:
    data = gh.json(
        f"pulls-for {repo} {sha}",
        [
            "api",
            "-H",
            "Accept: application/vnd.github+json",
            f"repos/{repo}/commits/{sha}/pulls",
        ],
    )
    return data or []


def _state_label(info: dict) -> str:
    state = (info.get("state") or "").lower()
    if state == "open" and info.get("isDraft"):
        return "open draft"
    if state == "merged" or info.get("mergedAt"):
        return "merged"
    return state or "unknown"


def _distinctiveness(rel: str) -> tuple:
    base = rel.rsplit("/", 1)[-1]
    penalty = 50 if base in LOW_VALUE_NAMES else 0
    return (penalty, -len(base), rel)


@dataclass
class PrHit:
    repo: str
    number: int
    title: str
    state: str
    url: str
    in_baseline: bool
    note: str
    files: list[str] = field(default_factory=list)


def _record(hits: dict[tuple[str, int], PrHit], hit: PrHit, rel: str) -> None:
    key = (hit.repo, hit.number)
    current = hits.get(key)
    if current is None:
        hit.files = [rel]
        hits[key] = hit
        return
    if rel not in current.files:
        current.files.append(rel)
    if hit.in_baseline:
        current.in_baseline = True
    if hit.note and hit.note not in current.note:
        current.note = (current.note + " " + hit.note).strip()


def attribute_prs(
    image_files: dict[str, FileRec],
    baselines: dict[str, str],
    gh: GhCache,
) -> dict:
    candidates: dict[str, FileRec] = {}
    for path, rec in image_files.items():
        if ignored_path(path, include_pyc=False, include_app=False):
            continue
        rel = site_rel(path) or path.lstrip("/")
        if repo_for_rel(rel) is None and not rel.endswith("patch_current_vllm.py"):
            continue
        # Later layers already overwrote earlier ones. Keep source files only.
        if rel.endswith(".pyc") or "/__pycache__/" in f"/{rel}":
            continue
        candidates[rel] = rec

    same_baseline: set[str] = set()
    for rel, rec in candidates.items():
        mapped = repo_for_rel(rel)
        if mapped is None:
            continue
        repo, git_path = mapped
        ref = baselines.get(repo)
        if not ref:
            continue
        eprint(f"baseline {repo}@{ref} {git_path}")
        blob = file_at_ref(gh, repo, git_path, ref)
        if blob is not None and norm_sha(blob) == rec.norm_sha:
            same_baseline.add(rel)

    hits: dict[tuple[str, int], PrHit] = {}
    attributed: set[str] = set()
    referenced: list[tuple[str, int, str]] = []

    def take_pr(repo: str, number: int, rels: list[str], force_in_baseline: bool | None = None) -> bool:
        info = pr_info(gh, repo, number)
        if not info:
            return False
        merge_sha = (info.get("mergeCommit") or {}).get("oid") or ""
        ref = baselines.get(repo)
        in_base = False
        note = ""
        if force_in_baseline is not None:
            in_base = force_in_baseline
        elif ref and merge_sha and (info.get("state") == "MERGED" or info.get("mergedAt")):
            flag = merged_into(gh, repo, ref, merge_sha)
            in_base = bool(flag)
            if flag is False:
                note = f"merged, not an ancestor of {ref}"
        elif any(rel in same_baseline for rel in rels):
            in_base = True
        if in_base and any(rel not in same_baseline for rel in rels):
            note = (
                note + " image matches this pull request; the baseline ref has later edits"
            ).strip()
        hit = PrHit(
            repo=repo,
            number=number,
            title=info.get("title") or "",
            state=_state_label(info),
            url=info.get("url") or f"https://github.com/{repo}/pull/{number}",
            in_baseline=in_base,
            note=note,
        )
        for rel in rels:
            _record(hits, hit, rel)
            attributed.add(rel)
        body = info.get("body") or ""
        for mentioned_repo, mentioned_num in pull_mentions(body[:4000]):
            if (mentioned_repo, mentioned_num) == (repo, number):
                continue
            referenced.append((mentioned_repo, mentioned_num, f"named in {repo}#{number}"))
        return True

    def pr_matches_file(repo: str, number: int, rel: str, rec: FileRec) -> list[str]:
        info = pr_info(gh, repo, number)
        paths = [item.get("path") or "" for item in (info or {}).get("files") or []]
        if rel not in paths:
            return []
        blob = file_at_ref(gh, repo, rel, f"refs/pull/{number}/head")
        if blob is None or norm_sha(blob) != rec.norm_sha:
            return []
        matched = [rel]
        if not info:
            return matched
        for other_rel, other in candidates.items():
            if other_rel == rel or other_rel in attributed:
                continue
            mapped = repo_for_rel(other_rel)
            if mapped is None or mapped[0] != repo:
                continue
            if other_rel not in paths:
                continue
            other_blob = file_at_ref(gh, repo, other_rel, f"refs/pull/{number}/head")
            if other_blob is not None and norm_sha(other_blob) == other.norm_sha:
                matched.append(other_rel)
        return matched

    search_order = sorted(
        (rel for rel in candidates if repo_for_rel(rel) and rel not in same_baseline),
        key=_distinctiveness,
    )
    for rel in search_order:
        if rel in attributed:
            continue
        repo, _git_path = repo_for_rel(rel)  # type: ignore[misc]
        rec = candidates[rel]
        base = rel.rsplit("/", 1)[-1]
        queries = []
        if base not in LOW_VALUE_NAMES and len(base) >= 8:
            queries.append(base.replace(".py", ""))
        queries.extend(rare_tokens(rec.data)[:1])
        eprint(f"search {repo} for {rel}")
        found = False
        seen_numbers: set[int] = set()
        for query in queries:
            for item in search_prs(gh, repo, query):
                number = int(item["number"])
                if number in seen_numbers:
                    continue
                seen_numbers.add(number)
                matched = pr_matches_file(repo, number, rel, rec)
                if not matched:
                    continue
                eprint(f"  match {repo}#{number} ({len(matched)} files)")
                take_pr(repo, number, matched)
                found = True
                break
            if found:
                break

    for rel in sorted(same_baseline):
        if rel in attributed:
            continue
        repo, git_path = repo_for_rel(rel)  # type: ignore[misc]
        ref = baselines[repo]
        eprint(f"history {repo}@{ref} {git_path}")
        for sha in commits_touching(gh, repo, ref, git_path):
            pulls = pulls_for_commit(gh, repo, sha)
            if not pulls:
                continue
            number = int(pulls[0]["number"])
            take_pr(repo, number, [rel], force_in_baseline=True)
            break

    already_without_pr = sorted(rel for rel in same_baseline if rel not in attributed)
    unpublished = []
    for rel, rec in sorted(candidates.items()):
        if rel in attributed or rel in same_baseline:
            continue
        mentions = pull_mentions(normalize(rec.data).decode("utf-8", "replace")[:5000])
        unpublished.append({"path": rel, "mentions": [f"{repo}#{num}" for repo, num in mentions]})
        for repo, num in mentions:
            referenced.append((repo, num, f"named in {rel}"))

    related = []
    seen_rel: set[tuple[str, int]] = set()
    for repo, number, why in referenced:
        if (repo, number) in hits or (repo, number) in seen_rel:
            continue
        seen_rel.add((repo, number))
        info = pr_info(gh, repo, number)
        if not info:
            continue
        ref = baselines.get(repo)
        merge_sha = (info.get("mergeCommit") or {}).get("oid") or ""
        in_base = False
        if ref and merge_sha and (info.get("mergedAt") or info.get("state") == "MERGED"):
            in_base = bool(merged_into(gh, repo, ref, merge_sha))
        related.append(
            {
                "repo": repo,
                "number": number,
                "title": info.get("title") or "",
                "state": _state_label(info),
                "url": info.get("url") or f"https://github.com/{repo}/pull/{number}",
                "in_baseline": in_base,
                "why": why,
            }
        )

    def dump(hit: PrHit) -> dict:
        return {
            "repo": hit.repo,
            "number": hit.number,
            "title": hit.title,
            "state": hit.state,
            "url": hit.url,
            "in_baseline": hit.in_baseline,
            "note": hit.note,
            "files": hit.files,
        }

    unmerged = [dump(hit) for hit in hits.values() if not hit.in_baseline and hit.state != "merged"]
    merged_not_in = [
        dump(hit)
        for hit in hits.values()
        if not hit.in_baseline and hit.state == "merged"
    ]
    already = [dump(hit) for hit in hits.values() if hit.in_baseline]
    for group in (unmerged, merged_not_in, already):
        group.sort(key=lambda item: (item["repo"], item["number"]))
    return {
        "baselines": baselines,
        "unmerged": unmerged,
        "merged_not_in_baseline": merged_not_in,
        "already_in_baseline": already,
        "already_without_pr": already_without_pr,
        "unpublished": unpublished,
        "related": related,
    }


def render_prs(image1: ImageInfo, image2: ImageInfo, report: dict) -> str:
    lines: list[str] = []
    base_bits = [f"`{repo}` @ `{ref}`" for repo, ref in report["baselines"].items()]
    baseline = ", ".join(base_bits) if base_bits else "no git baseline (pass --aiter and --vllm-ref)"
    lines.append(f"# {image1.name} vs {image2.name}")
    lines.append("")
    lines.append(f"Images: `{image1.ref.raw}` vs `{image2.ref.raw}`.")
    lines.append(f"Baseline: {baseline}.")
    lines.append(
        "A pull request is unmerged when the image bytes match it and that change is not in the baseline ref. "
        "Files already present at the baseline ref are listed with the pull request that last set those bytes."
    )
    lines.append("")

    def section(title: str, rows: list[dict]) -> None:
        lines.append(f"## {title}")
        lines.append("")
        if not rows:
            lines.append("(none)")
            lines.append("")
            return
        for row in rows:
            label = row["repo"].split("/")[-1]
            draft = row["state"]
            note = f" {row['note']}" if row.get("note") else ""
            lines.append(
                f"- [{label} #{row['number']}]({row['url']}) — {draft}.{note} {row['title']}"
            )
            files = ", ".join(f"`{path}`" for path in row["files"][:12])
            extra = ""
            if len(row["files"]) > 12:
                extra = f" (+{len(row['files']) - 12} more)"
            lines.append(f"  - files: {files}{extra}")
        lines.append("")

    section("Unmerged, in the image and not in the baseline", report["unmerged"])
    section("Merged upstream, not in this baseline", report["merged_not_in_baseline"])
    section("Already in the baseline", report["already_in_baseline"])
    if report.get("already_without_pr"):
        lines.append("Bytes match the baseline, and no pull request was resolved:")
        lines.append("")
        for path in report["already_without_pr"]:
            lines.append(f"- `{path}`")
        lines.append("")

    lines.append("## No public pull request has these bytes")
    lines.append("")
    if not report["unpublished"]:
        lines.append("(none)")
        lines.append("")
    else:
        for item in report["unpublished"]:
            mention = ""
            if item["mentions"]:
                mention = " — mentions " + ", ".join(item["mentions"])
            lines.append(f"- `{item['path']}`{mention}")
        lines.append("")

    lines.append("## Referenced by a matched pull request or file")
    lines.append("")
    if not report["related"]:
        lines.append("(none)")
        lines.append("")
    else:
        for row in report["related"]:
            label = row["repo"].split("/")[-1]
            where = "in the baseline" if row["in_baseline"] else "not in the baseline"
            lines.append(
                f"- [{label} #{row['number']}]({row['url']}) — {row['state']}, {where}. {row['title']} ({row['why']})"
            )
        lines.append("")
    return "\n".join(lines).rstrip() + "\n"


def infer_baselines(image2: ImageRef, aiter_ref: str | None, vllm_ref: str | None, customizations: list[Customize]) -> dict[str, str]:
    baselines: dict[str, str] = {}
    if vllm_ref:
        baselines["vllm-project/vllm"] = vllm_ref
    elif re.fullmatch(r"v?\d+\.\d+\.\d+[\w.-]*", image2.tag):
        baselines["vllm-project/vllm"] = image2.tag if image2.tag.startswith("v") else "v" + image2.tag
    for cust in customizations:
        if cust.package == "aiter" and cust.ref and "ROCm/aiter" not in baselines:
            baselines["ROCm/aiter"] = cust.ref
        if cust.package == "vllm" and cust.ref:
            baselines["vllm-project/vllm"] = cust.ref
    if aiter_ref:
        baselines["ROCm/aiter"] = aiter_ref
    return baselines


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Compare two container images and optional trees installed on top.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=textwrap.dedent(
            """\
            examples:
              python compare_docker_images.py IMAGE1 vllm/vllm-openai-rocm:v0.32.0 --aiter v0.1.26 --out report.md
              python compare_docker_images.py IMAGE1 IMAGE2 --aiter v0.1.26 --offline --out report.md
            """
        ),
    )
    parser.add_argument("image1", nargs="?", help="first image (ref or name:tag)")
    parser.add_argument("image2", nargs="?", help="second image")
    parser.add_argument("--name1", default="image1")
    parser.add_argument("--name2", default="image2")
    parser.add_argument(
        "--aiter",
        help="AITER release treated as installed on image 2, for example v0.1.26",
    )
    parser.add_argument(
        "--vllm-ref",
        help="vLLM git ref for the baseline. Defaults to image 2's tag when it is a version.",
    )
    parser.add_argument(
        "--customize",
        action="append",
        default=[],
        help="Optional local tree TARGET:PACKAGE=SOURCE[@REF]. "
        "An @REF here also sets that package's baseline.",
    )
    parser.add_argument(
        "--files",
        action="store_true",
        help="also write the raw layer file listing",
    )
    parser.add_argument("--engine", choices=("auto", "registry", "docker"), default="auto")
    parser.add_argument("--platform", default="linux/amd64")
    parser.add_argument("--tail-layers", type=int, default=40)
    parser.add_argument("--max-layer-mb", type=float, default=8.0)
    parser.add_argument("--max-diff-lines", type=int, default=40)
    parser.add_argument(
        "--include-pyc",
        action="store_true",
        help="include __pycache__ and .pyc files in the file delta",
    )
    parser.add_argument(
        "--include-app",
        action="store_true",
        help="include /app/vllm benchmarks, examples, and docker files",
    )
    parser.add_argument(
        "--cache",
        type=Path,
        default=Path(__file__).resolve().parent / "cache",
    )
    parser.add_argument("--offline", action="store_true")
    parser.add_argument("--out", type=Path, help="write the markdown report here")
    parser.add_argument("--json", type=Path, help="write the machine-readable report here")
    parser.add_argument("--self-test", action="store_true")
    args = parser.parse_args(argv)

    if args.self_test:
        self_test()
        return 0
    if not args.image1 or not args.image2:
        parser.error("image1 and image2 are required")

    customizations = [parse_customize(item) for item in args.customize]
    for cust in customizations:
        prepare_customize(cust)

    cache = Cache(args.cache, offline=args.offline)
    max_bytes = int(args.max_layer_mb * 1000 * 1000)
    image1 = load_image(
        parse_image_ref(args.image1),
        args.name1,
        cache,
        args.engine,
        args.platform,
        args.tail_layers,
        max_bytes,
    )
    image2 = load_image(
        parse_image_ref(args.image2),
        args.name2,
        cache,
        args.engine,
        args.platform,
        args.tail_layers,
        max_bytes,
    )
    baselines = infer_baselines(image2.ref, args.aiter, args.vllm_ref, customizations)
    if not baselines:
        raise SystemExit(
            "No git baseline. Pass --aiter v0.1.26, or use a version tag for image 2 "
            "(vllm/vllm-openai-rocm:v0.32.0)."
        )
    if shutil.which("gh") is None:
        raise SystemExit("gh is required to resolve pull requests.")
    gh = GhCache(args.cache, offline=args.offline)
    payload = attribute_prs(image1.files, baselines, gh)
    text = render_prs(image1, image2, payload)
    if args.files:
        file_text, file_payload = render(
            image1,
            image2,
            customizations,
            args.max_diff_lines,
            include_pyc=args.include_pyc,
            include_app=args.include_app,
        )
        text += "\n" + file_text
        payload["files"] = file_payload["files"]
    if args.out:
        args.out.parent.mkdir(parents=True, exist_ok=True)
        args.out.write_text(text, encoding="utf-8")
        eprint(f"wrote {args.out}")
    else:
        sys.stdout.write(text)
    json_path = args.json
    if json_path is None and args.out:
        json_path = args.out.with_suffix(".json")
    if json_path:
        json_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        eprint(f"wrote {json_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
