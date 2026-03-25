"""
Fetch PRs authored by a list of GitHub users and export to Excel.

Edit the CONFIG section below, then run:
  python fetch-team-prs.py

Notes:
  - Uses Search API to find PRs, then Pulls API to enrich status (merged/closed/etc).
  - If INCLUDE_CHECKS=True, it also fetches CI/check status (more API calls).
  - GitHub Search API returns at most 1000 results per query; if you suspect truncation,
    narrow with ORG / REPOS / UPDATED_SINCE_DAYS / CLOSED_WITHIN_DAYS.
"""

from __future__ import annotations

import dataclasses
import datetime as dt
import math
import os
import re
import sys
import time
from numbers import Integral, Real
from typing import Any, Dict, Iterable, List, Optional, Tuple

import pandas as pd
import requests
import yaml


GITHUB_API_DEFAULT = "https://api.github.com"

#
# =========================
# CONFIG (edit as needed)
# =========================
#

# Prefer setting env var GITHUB_TOKEN instead of hardcoding a token here.
# If you must hardcode it, replace "" with your token string.
TOKEN: str = ""

# GitHub usernames to fetch PRs/issues for. Can also set GITHUB_USERS env var (comma-separated).
USERS: List[str] = []

# Load from config.yaml if present
CONFIG_YAML_PATH = "config.yaml"
if "--config" in sys.argv:
    try:
        idx = sys.argv.index("--config")
        CONFIG_YAML_PATH = sys.argv[idx + 1]
    except IndexError:
        pass

if os.path.exists(CONFIG_YAML_PATH):
    with open(CONFIG_YAML_PATH, "r", encoding="utf-8") as f:
        try:
            config_data = yaml.safe_load(f) or {}
            if "TOKEN" in config_data:
                TOKEN = config_data["TOKEN"]
            if "USERS" in config_data:
                USERS = config_data["USERS"]
        except Exception as e:
            print(f"WARNING: Failed to parse {CONFIG_YAML_PATH}: {e}", file=sys.stderr)

if not TOKEN:
    TOKEN = os.environ.get("GITHUB_TOKEN", "")

if not USERS:
    env_users = os.environ.get("GITHUB_USERS", "")
    USERS = [u.strip() for u in env_users.split(",") if u.strip()]

# Include issues opened by team members (in addition to PRs).
INCLUDE_ISSUES: bool = True

# One of: "open", "closed", "all"
STATE: str = "all"

# Optional org filter for search (e.g. "my-org"). Use None to disable.
ORG: Optional[str] = None

# Optional repo filters like ["owner1/repoA", "owner2/repoB"].
REPOS: List[str] = []

# Owners/orgs to exclude from the Excel output (case-insensitive), e.g. ["ROCm", "my-org"]
HIDE_ORGS: List[str] = ["openvinotoolkit"]

# For open items, how far back to search by "updated". Set to 0 to disable.
UPDATED_SINCE_DAYS: int = 365

# For closed items (STATE="closed"), include only those closed within last N days. Set to 0 to disable.
# Note: for PRs with STATE="all", closed PRs are included based on UPDATED_SINCE_DAYS (updated:>=...),
# so older merged PRs aren't missed.
CLOSED_WITHIN_DAYS: int = 30

# Search API page size (max 100). Larger = fewer requests.
PER_PAGE: int = 100

# If True, fetch combined status + check-runs summary for head SHA (more API calls).
INCLUDE_CHECKS: bool = False

# GitHub API base (useful for GHES). Leave default for github.com.
API_BASE: str = GITHUB_API_DEFAULT

# Output Excel path
OUT_XLSX: str = "team_prs_ww13.xlsx"

# Optional: previous week's exported Excel to diff against.
PREVIOUS_WEEK_XLSX: str = "team_prs_ww12.xlsx"

# Weekly "updated recently" tab (requested: named "Weekly Summary")
WEEKLY_UPDATED_DAYS: int = 7
WEEKLY_SUMMARY_SHEET: str = "Weekly Summary"

# All issues tab (full list)
ISSUES_SHEET: str = "Issues"

# All PRs tab (full list)
PRS_SHEET: str = "PR during year"

# PRs closed within CLOSED_WITHIN_DAYS (filtered view)
PRS_UPDATED_SHEET: str = "PRs updated last 30 days"

# Issues report tab (subset of columns)
REPORT_SHEET: str = "Report"

# Week-over-week delta tab (transitions + new items)
WEEKLY_DELTA_SHEET: str = "Weekly Delta"

# Open PRs tab (all open PRs, columns: author, repo, url, updated_at)
OPEN_PRS_SHEET: str = "Open PRs"

# If OUT_XLSX already exists (and PREVIOUS_WEEK_XLSX exists), do delta-only mode:
# - do NOT call GitHub APIs
# - read OUT_XLSX as "current", read PREVIOUS_WEEK_XLSX as "previous"
# - (re)write OUT_XLSX with Current + Weekly Summary (+ Weekly Delta) sheets
DELTA_ONLY_IF_OUT_EXISTS: bool = True

# If Search API misses private PRs, enable this to also scan configured REPOS via Pulls API.
# (Only scans repos listed in REPOS, not "all repos you can access".)
FALLBACK_SCAN_REPOS: bool = True


def _utc_now() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


def _parse_iso8601(s: Optional[str]) -> Optional[dt.datetime]:
    if not s:
        return None
    # GitHub returns "2026-01-01T12:34:56Z"
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    return dt.datetime.fromisoformat(s)


def _fmt_dt(d: Optional[dt.datetime]) -> Optional[str]:
    if not d:
        return None
    return d.astimezone(dt.timezone.utc).isoformat().replace("+00:00", "Z")


def _sleep_backoff(attempt: int) -> None:
    # Exponential backoff with cap
    seconds = min(60, 2 ** attempt)
    time.sleep(seconds)


class GitHubApiError(RuntimeError):
    pass


@dataclasses.dataclass(frozen=True)
class RepoRef:
    owner: str
    repo: str

    @property
    def full_name(self) -> str:
        return f"{self.owner}/{self.repo}"


def _repo_ref_from_html_url(html_url: str) -> Optional[RepoRef]:
    # e.g. https://github.com/OWNER/REPO/pull/123 or https://github.com/OWNER/REPO/issues/123
    m = re.match(r"^https?://github\.com/([^/]+)/([^/]+)/(?:pull|issues)/\d+/?$", html_url)
    if not m:
        return None
    return RepoRef(owner=m.group(1), repo=m.group(2))


def _pr_number_from_html_url(html_url: str) -> Optional[int]:
    m = re.match(r"^https?://github\.com/[^/]+/[^/]+/(?:pull|issues)/(\d+)/?$", html_url)
    if not m:
        return None
    return int(m.group(1))


def _join_list(values: Iterable[str]) -> str:
    vals = [v for v in values if v]
    return ", ".join(vals)


class GitHubClient:
    def __init__(self, token: str, api_base: str = GITHUB_API_DEFAULT, user_agent: str = "team-prs-exporter"):
        self.api_base = api_base.rstrip("/")
        self.session = requests.Session()

        # GitHub classic PATs commonly start with "ghp_".
        # Fine-grained PATs commonly start with "github_pat_".
        # Using the token scheme for classic PATs avoids edge-case auth issues.
        scheme = "Bearer" if token.startswith("github_pat_") else "token"
        self.session.headers.update(
            {
                "Accept": "application/vnd.github+json",
                "Authorization": f"{scheme} {token}",
                "X-GitHub-Api-Version": "2022-11-28",
                "User-Agent": user_agent,
            }
        )

    def request_json(self, method: str, url: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        # Handles secondary rate limits with retry/backoff.
        attempt = 0
        while True:
            resp = self.session.request(method, url, params=params, timeout=60)
            if resp.status_code in (200, 201):
                return resp.json()

            # Rate limiting
            if resp.status_code == 403:
                remaining = resp.headers.get("X-RateLimit-Remaining")
                reset = resp.headers.get("X-RateLimit-Reset")
                msg = None
                try:
                    msg = resp.json().get("message")
                except Exception:
                    msg = resp.text

                # Primary rate limit hit: wait until reset
                if remaining == "0" and reset:
                    reset_ts = int(reset)
                    wait_s = max(1, reset_ts - int(time.time()) + 1)
                    print(
                        f"INFO: rate limit hit for {method} {url}; sleeping {min(wait_s, 600)}s (reset={reset_ts}, attempt={attempt})",
                        file=sys.stderr,
                    )
                    time.sleep(min(wait_s, 600))
                    attempt += 1
                    continue

                # Secondary rate limit / abuse detection: backoff
                if msg and ("secondary rate limit" in msg.lower() or "abuse detection" in msg.lower()):
                    print(
                        f"INFO: secondary rate limit for {method} {url}; backoff (attempt={attempt})",
                        file=sys.stderr,
                    )
                    _sleep_backoff(attempt)
                    attempt += 1
                    if attempt <= 8:
                        continue

            # Add request context to help debug (especially for Search API 422 errors).
            param_str = ""
            try:
                if params:
                    param_str = f" params={params!r}"
            except Exception:
                param_str = ""
            raise GitHubApiError(f"GitHub API error {resp.status_code} for {method} {url}{param_str}: {resp.text}")

    def paginate(self, url: str, params: Dict[str, Any]) -> Iterable[Dict[str, Any]]:
        page = 1
        while True:
            p = dict(params)
            p["page"] = page
            data = self.request_json("GET", url, params=p)
            items = data.get("items", [])
            if not items:
                return
            for it in items:
                yield it
            # Search API maxes at 1000 results; we stop when fewer than per_page returned
            if len(items) < int(params.get("per_page", 30)):
                return
            page += 1

    def search_prs(self, q: str, per_page: int = 100) -> Iterable[Dict[str, Any]]:
        url = f"{self.api_base}/search/issues"
        params = {"q": q, "sort": "updated", "order": "desc", "per_page": per_page}
        return self.paginate(url, params=params)

    def get_pr(self, pr_api_url: str) -> Dict[str, Any]:
        return self.request_json("GET", pr_api_url)

    def get_repo(self, full_name: str) -> Optional[Dict[str, Any]]:
        # Returns None if not found or not accessible.
        if "/" not in full_name:
            return None
        owner, repo = full_name.split("/", 1)
        url = f"{self.api_base}/repos/{owner}/{repo}"
        resp = self.session.request("GET", url, timeout=60)
        if resp.status_code == 200:
            return resp.json()
        if resp.status_code in (403, 404):
            return None
        raise GitHubApiError(f"GitHub API error {resp.status_code} for GET {url}: {resp.text}")

    def get_org(self, org: str) -> Optional[Dict[str, Any]]:
        # Returns None if not found or not accessible.
        url = f"{self.api_base}/orgs/{org}"
        resp = self.session.request("GET", url, timeout=60)
        if resp.status_code == 200:
            return resp.json()
        if resp.status_code in (403, 404):
            return None
        raise GitHubApiError(f"GitHub API error {resp.status_code} for GET {url}: {resp.text}")

    def get_combined_status(self, repo: RepoRef, sha: str) -> Dict[str, Any]:
        # Older "combined status" endpoint; still useful for a quick green/red/pending.
        url = f"{self.api_base}/repos/{repo.owner}/{repo.repo}/commits/{sha}/status"
        return self.request_json("GET", url)

    def get_check_runs(self, repo: RepoRef, sha: str) -> Dict[str, Any]:
        url = f"{self.api_base}/repos/{repo.owner}/{repo.repo}/commits/{sha}/check-runs"
        return self.request_json("GET", url)

    def list_pulls(
        self,
        repo: RepoRef,
        state: str,
        per_page: int = 100,
        sort: str = "updated",
        direction: str = "desc",
    ) -> Iterable[Dict[str, Any]]:
        url = f"{self.api_base}/repos/{repo.owner}/{repo.repo}/pulls"
        page = 1
        while True:
            params = {
                "state": state,
                "per_page": per_page,
                "page": page,
                "sort": sort,
                "direction": direction,
            }
            resp = self.session.request("GET", url, params=params, timeout=60)
            if resp.status_code != 200:
                raise GitHubApiError(f"GitHub API error {resp.status_code} for GET {url} params={params!r}: {resp.text}")
            items = resp.json()
            if not items:
                return
            for it in items:
                yield it
            if len(items) < int(per_page):
                return
            page += 1


def build_search_query(
    author: str,
    state: str,
    org: Optional[str],
    repos: List[str],
    updated_since: Optional[dt.datetime],
    closed_since: Optional[dt.datetime],
    item_type: str,  # "pr" or "issue"
) -> str:
    if item_type not in ("pr", "issue"):
        raise ValueError(f"Unsupported item_type: {item_type}")

    q_parts = [f"is:{item_type}", f"author:{author}"]

    if state == "open":
        q_parts.append("is:open")
    elif state == "closed":
        q_parts.append("is:closed")
    elif state == "all":
        pass
    else:
        raise ValueError(f"Unsupported state: {state}")

    if org:
        q_parts.append(f"org:{org}")

    if repos:
        # Search supports multiple repo qualifiers
        for r in repos:
            q_parts.append(f"repo:{r}")

    if updated_since:
        # Search qualifier: updated:>=YYYY-MM-DD
        q_parts.append(f"updated:>={updated_since.date().isoformat()}")

    if closed_since and state == "closed":
        # Search qualifier: closed:>=YYYY-MM-DD (only meaningful for closed search)
        q_parts.append(f"closed:>={closed_since.date().isoformat()}")

    return " ".join(q_parts)


def pr_status_from_pr(pr: Dict[str, Any]) -> str:
    # Explicit merged detection
    if pr.get("merged_at"):
        return "merged"
    if pr.get("state") == "open":
        return "open"
    return "closed"


def extract_pr_row(
    *,
    author: str,
    issue: Dict[str, Any],
    pr: Dict[str, Any],
    include_checks: bool,
    gh: GitHubClient,
) -> Dict[str, Any]:
    html_url = issue.get("html_url") or pr.get("html_url")
    repo_ref = _repo_ref_from_html_url(html_url or "")
    number = _pr_number_from_html_url(html_url or "") or pr.get("number") or issue.get("number")

    labels = _join_list([l.get("name", "") for l in (issue.get("labels") or []) if isinstance(l, dict)])
    assignees = _join_list([a.get("login", "") for a in (issue.get("assignees") or []) if isinstance(a, dict)])

    requested_reviewers = _join_list(
        [u.get("login", "") for u in (pr.get("requested_reviewers") or []) if isinstance(u, dict)]
    )
    requested_teams = _join_list(
        [t.get("name", "") for t in (pr.get("requested_teams") or []) if isinstance(t, dict)]
    )

    head = pr.get("head") or {}
    base = pr.get("base") or {}
    head_sha = (head.get("sha") or "") if isinstance(head, dict) else ""
    head_ref = (head.get("ref") or "") if isinstance(head, dict) else ""
    base_ref = (base.get("ref") or "") if isinstance(base, dict) else ""

    status = pr_status_from_pr(pr)
    # Draft PRs are filtered out at collection time.

    ci_state = None
    checks_summary = None
    if include_checks and repo_ref and head_sha:
        try:
            combined = gh.get_combined_status(repo_ref, head_sha)
            ci_state = combined.get("state")
        except Exception:
            ci_state = None

        try:
            checks = gh.get_check_runs(repo_ref, head_sha)
            total = int(checks.get("total_count", 0) or 0)
            runs = checks.get("check_runs", []) or []
            conclusions = {}
            for r in runs:
                conclusion = r.get("conclusion") or r.get("status") or "unknown"
                conclusions[conclusion] = conclusions.get(conclusion, 0) + 1
            # e.g. "total=12; completed=10; success=9; failure=1; in_progress=2"
            completed = sum(1 for r in runs if (r.get("status") == "completed"))
            parts = [f"total={total}", f"completed={completed}"]
            for k in sorted(conclusions.keys()):
                parts.append(f"{k}={conclusions[k]}")
            checks_summary = "; ".join(parts)
        except Exception:
            checks_summary = None

    return {
        "author": author,
        "item_type": "pr",
        "repo": repo_ref.full_name if repo_ref else None,
        "number": number,
        "title": issue.get("title") or pr.get("title"),
        "url": html_url,
        "status": status,  # open/closed/merged
        "state": pr.get("state"),
        "created_at": pr.get("created_at") or issue.get("created_at"),
        "updated_at": pr.get("updated_at") or issue.get("updated_at"),
        "closed_at": pr.get("closed_at") or issue.get("closed_at"),
        "merged_at": pr.get("merged_at"),
        "base_ref": base_ref,
        "head_ref": head_ref,
        "head_sha": head_sha or None,
        "labels": labels,
        "assignees": assignees,
        "requested_reviewers": requested_reviewers,
        "requested_teams": requested_teams,
        "comments": issue.get("comments"),
        "review_comments": pr.get("review_comments"),
        "commits": pr.get("commits"),
        "additions": pr.get("additions"),
        "deletions": pr.get("deletions"),
        "changed_files": pr.get("changed_files"),
        "mergeable_state": pr.get("mergeable_state"),
        "ci_state": ci_state,
        "checks_summary": checks_summary,
    }


def _extract_issue_row(*, author: str, issue: Dict[str, Any]) -> Dict[str, Any]:
    html_url = issue.get("html_url")
    repo_ref = _repo_ref_from_html_url(html_url or "")
    number = _pr_number_from_html_url(html_url or "") or issue.get("number")

    labels = _join_list([l.get("name", "") for l in (issue.get("labels") or []) if isinstance(l, dict)])
    assignees = _join_list([a.get("login", "") for a in (issue.get("assignees") or []) if isinstance(a, dict)])

    state = issue.get("state")
    status = "closed" if state == "closed" else "open"

    return {
        "author": author,
        "item_type": "issue",
        "repo": repo_ref.full_name if repo_ref else None,
        "number": number,
        "title": issue.get("title"),
        "url": html_url,
        "status": status,
        "state": state,
        "created_at": issue.get("created_at"),
        "updated_at": issue.get("updated_at"),
        "closed_at": issue.get("closed_at"),
        "merged_at": None,
        "base_ref": None,
        "head_ref": None,
        "head_sha": None,
        "labels": labels,
        "assignees": assignees,
        "requested_reviewers": None,
        "requested_teams": None,
        "ci_state": None,
        "checks_summary": None,
        "comments": issue.get("comments"),
        "review_comments": None,
        "commits": None,
        "additions": None,
        "deletions": None,
        "changed_files": None,
        "mergeable_state": None,
    }


def _state_splits_for_all(state: str) -> List[str]:
    # For STATE="all", run separate searches for open and closed
    # so CLOSED_WITHIN_DAYS can be applied only to closed results.
    if state == "all":
        return ["open", "closed"]
    return [state]


def _is_nan(v: Any) -> bool:
    try:
        return bool(pd.isna(v))
    except Exception:
        return False


def _to_cmp_str(v: Any) -> str:
    if v is None or _is_nan(v):
        return ""
    # Pandas may materialize integer-ish columns as floats (e.g. 5.0) when NaNs exist.
    # For week-over-week transitions we want "5 --> 6", not "5.0 --> 6.0".
    if isinstance(v, str):
        s = v.strip()
        if re.fullmatch(r"-?\d+\.0+", s):
            try:
                return str(int(float(s)))
            except Exception:
                return s
        return s

    # Avoid treating bool as numeric.
    if isinstance(v, bool):
        return str(v)

    if isinstance(v, Integral):
        return str(int(v))

    if isinstance(v, Real):
        fv = float(v)
        if math.isfinite(fv) and fv.is_integer():
            return str(int(fv))
        return str(fv)

    return str(v)


def _transition(old: Any, new: Any) -> str:
    o = _to_cmp_str(old)
    n = _to_cmp_str(new)
    if o == n:
        return ""
    if not o and n:
        return f"--> {n}"
    if o and not n:
        return f"{o} -->"
    return f"{o} --> {n}"

def reorder_columns_author_url_first(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    cols = list(df.columns)
    lead = [c for c in ["author", "url"] if c in cols]
    rest = [c for c in cols if c not in lead]
    return df[lead + rest]


def build_weekly_summary(
    current_df: pd.DataFrame,
    previous_df: pd.DataFrame,
    *,
    key_col: str = "url",
    tracked_cols: Tuple[str, ...] = ("commits", "comments", "state", "status"),
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Returns (changes_df, new_items_df).
      - changes_df: items present in both current and previous where any tracked col changed,
        with transitions formatted as "old --> new".
      - new_items_df: items present in current but not previous.
    """
    cur = current_df.copy()
    prev = previous_df.copy()

    if key_col not in cur.columns or key_col not in prev.columns:
        return pd.DataFrame(), pd.DataFrame()

    cur[key_col] = cur[key_col].astype(str)
    prev[key_col] = prev[key_col].astype(str)

    cur_idx = cur.set_index(key_col, drop=False)
    prev_idx = prev.set_index(key_col, drop=False)

    common_keys = cur_idx.index.intersection(prev_idx.index)
    new_keys = cur_idx.index.difference(prev_idx.index)

    base_cols = [c for c in ["author", "url", "item_type", "repo", "number", "title"] if c in cur.columns]

    change_rows: List[Dict[str, Any]] = []
    for k in common_keys:
        cur_row = cur_idx.loc[k]
        prev_row = prev_idx.loc[k]
        if isinstance(cur_row, pd.DataFrame):
            cur_row = cur_row.iloc[0]
        if isinstance(prev_row, pd.DataFrame):
            prev_row = prev_row.iloc[0]

        out: Dict[str, Any] = {c: cur_row.get(c) for c in base_cols}
        changed_any = False
        for col in tracked_cols:
            if col not in cur.columns or col not in prev.columns:
                continue
            t = _transition(prev_row.get(col), cur_row.get(col))
            out[col] = t
            if t:
                changed_any = True
        if changed_any:
            change_rows.append(out)

    changes_df = pd.DataFrame(change_rows)

    new_items_df = cur_idx.loc[new_keys].copy()
    if isinstance(new_items_df, pd.Series):
        new_items_df = new_items_df.to_frame().T
    new_items_df = new_items_df.reset_index(drop=True)
    keep_new_cols = base_cols + [c for c in tracked_cols if c in new_items_df.columns]
    keep_new_cols = [c for c in keep_new_cols if c in new_items_df.columns]
    new_items_df = new_items_df[keep_new_cols] if keep_new_cols else pd.DataFrame()

    # Sort inside summary: item_type primary (issues then PRs), then author.
    def _sort_df(df: pd.DataFrame) -> pd.DataFrame:
        if df.empty or "item_type" not in df.columns:
            return df
        type_order = {"issue": 0, "pr": 1}
        df = df.copy()
        df["_type_sort"] = df["item_type"].apply(lambda x: type_order.get(str(x).lower(), 99))
        sort_cols = ["_type_sort"]
        if "author" in df.columns:
            sort_cols.append("author")
        df = df.sort_values(by=sort_cols, ascending=True, kind="mergesort").drop(columns=["_type_sort"])
        return df

    return (
        reorder_columns_author_url_first(_sort_df(changes_df)),
        reorder_columns_author_url_first(_sort_df(new_items_df)),
    )


def _load_export_df(path: str) -> pd.DataFrame:
    """
    Load a combined dataframe from an export workbook.
    Prefers PRs+Issues sheets if present; else falls back to Current; else first sheet.
    """
    xls = pd.ExcelFile(path)
    if PRS_SHEET in xls.sheet_names or ISSUES_SHEET in xls.sheet_names:
        parts = []
        if PRS_SHEET in xls.sheet_names:
            parts.append(pd.read_excel(path, sheet_name=PRS_SHEET))
        if ISSUES_SHEET in xls.sheet_names:
            parts.append(pd.read_excel(path, sheet_name=ISSUES_SHEET))
        if parts:
            return pd.concat(parts, ignore_index=True)
    if "Current" in xls.sheet_names:
        return pd.read_excel(path, sheet_name="Current")
    return pd.read_excel(path, sheet_name=0)


def build_weekly_updated_view(current_df: pd.DataFrame, days: int, *, item_type: Optional[str] = None) -> pd.DataFrame:
    if current_df.empty or "updated_at" not in current_df.columns:
        return pd.DataFrame()
    if days is None or int(days) <= 0:
        return pd.DataFrame()

    cutoff = _utc_now() - dt.timedelta(days=int(days))
    updated_key = current_df["updated_at"].apply(lambda x: _parse_iso8601(x) if isinstance(x, str) else None)
    mask = updated_key.apply(lambda d: bool(d) and d >= cutoff)
    dfw = current_df.loc[mask].copy()

    if item_type and "item_type" in dfw.columns:
        dfw = dfw[dfw["item_type"].astype(str).str.lower() == item_type.lower()].copy()

    # Keep useful columns first
    preferred = [
        "author",
        "repo",
        "number",
        "title",
        "url",
        "item_type",
        "status",
        "state",
        "updated_at",
        "closed_at",
        "merged_at",
        "comments",
        "commits",
    ]
    cols = [c for c in preferred if c in dfw.columns] + [c for c in dfw.columns if c not in preferred]
    dfw = dfw[cols]

    # Sort: author, updated desc (item_type is fixed if filtered)
    if "author" in dfw.columns:
        dfw["_updated_sort"] = dfw["updated_at"].apply(lambda x: _parse_iso8601(x) if isinstance(x, str) else None)
        dfw = dfw.sort_values(by=["author", "_updated_sort"], ascending=[True, False], kind="mergesort")
        dfw = dfw.drop(columns=["_updated_sort"])

    return reorder_columns_author_url_first(dfw)


def build_weekly_summary_view(weekly_df: pd.DataFrame) -> pd.DataFrame:
    """
    Weekly Summary tab requested columns:
      author, repo, url, updated_at
    """
    if weekly_df is None or weekly_df.empty:
        return pd.DataFrame()
    wanted = ["author", "repo", "url", "updated_at"]
    cols = [c for c in wanted if c in weekly_df.columns]
    return weekly_df[cols].copy() if cols else pd.DataFrame()


def build_prs_updated_recent_view(prs_df: pd.DataFrame, days: int) -> pd.DataFrame:
    """
    PRs updated tab: PRs updated within last N days (CLOSED_WITHIN_DAYS).
    If days <= 0, returns empty (no cutoff).
    """
    if prs_df is None or prs_df.empty:
        return pd.DataFrame()
    if days is None or int(days) <= 0:
        return pd.DataFrame()
    return build_weekly_updated_view(prs_df, int(days), item_type="pr").reset_index(drop=True)


def build_open_prs_view(prs_df: pd.DataFrame) -> pd.DataFrame:
    """
    Open PRs tab: all open PRs with columns author, repo, url, updated_at.
    """
    if prs_df is None or prs_df.empty:
        return pd.DataFrame()
    if "status" not in prs_df.columns:
        return pd.DataFrame()
    df = prs_df[prs_df["status"].astype(str).str.lower() == "open"].copy()
    if df.empty:
        return pd.DataFrame()
    wanted = ["author", "repo", "url", "updated_at"]
    cols = [c for c in wanted if c in df.columns]
    return reorder_columns_author_url_first(df[cols]) if cols else pd.DataFrame()


def split_prs_issues(df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    if df.empty or "item_type" not in df.columns:
        return df.copy(), pd.DataFrame()
    types = df["item_type"].astype(str).str.lower()
    prs = df.loc[types == "pr"].copy()
    issues = df.loc[types == "issue"].copy()
    return prs, issues


def build_issues_report_view(issues_df: pd.DataFrame) -> pd.DataFrame:
    """
    Report tab requested by user: Issues only, columns:
      author, repo, url, state, created_at, updated_at

    Additionally: exclude issues assigned to any of USERS.
    """
    if issues_df is None or issues_df.empty:
        return pd.DataFrame()

    wanted = ["author", "repo", "url", "state", "created_at", "updated_at"]
    cols = [c for c in wanted if c in issues_df.columns]
    out = issues_df.copy()

    # Skip issues assigned to team members.
    team_users = {u.strip().lower() for u in USERS if isinstance(u, str) and u.strip()}
    if team_users and "assignees" in out.columns:
        def _has_team_assignee(v: Any) -> bool:
            if v is None or _is_nan(v):
                return False
            if isinstance(v, str):
                parts = [p.strip().lower() for p in v.split(",") if p.strip()]
                return any(p in team_users for p in parts)
            if isinstance(v, (list, tuple, set)):
                return any(str(p).strip().lower() in team_users for p in v if p is not None)
            return False

        out = out.loc[~out["assignees"].apply(_has_team_assignee)].copy()

    # Keep only the requested columns (that exist).
    if cols:
        out = out[cols]

    # Sort requested: state (open first), then created_at desc (latest -> oldest).
    # Use parsed datetime so ordering is chronological, not string-based.
    sort_cols = []
    ascending_flags = []
    
    if "state" in out.columns:
        # "open" -> 0, "closed" -> 1
        out["_state_sort"] = out["state"].apply(lambda x: 0 if str(x).lower() == "open" else 1)
        sort_cols.append("_state_sort")
        ascending_flags.append(True)

    if "created_at" in out.columns:
        out["_created_sort"] = out["created_at"].apply(lambda x: _parse_iso8601(x) if isinstance(x, str) else None)
        sort_cols.append("_created_sort")
        ascending_flags.append(False)

    if sort_cols:
        out = out.sort_values(by=sort_cols, ascending=ascending_flags, kind="mergesort")
        drop_cols = [c for c in ["_state_sort", "_created_sort"] if c in out.columns]
        out = out.drop(columns=drop_cols)

    return out.reset_index(drop=True)


def _extract_pr_row_from_pr_only(
    *,
    author: str,
    pr: Dict[str, Any],
    include_checks: bool,
    gh: GitHubClient,
) -> Dict[str, Any]:
    # Build a minimal "issue-like" stub for reuse of extract_pr_row()
    issue_stub: Dict[str, Any] = {
        "title": pr.get("title"),
        "html_url": pr.get("html_url"),
        "number": pr.get("number"),
        "labels": pr.get("labels") or [],
        "assignees": pr.get("assignees") or [],
        "comments": pr.get("comments"),
        "created_at": pr.get("created_at"),
        "updated_at": pr.get("updated_at"),
        "closed_at": pr.get("closed_at"),
    }
    return extract_pr_row(author=author, issue=issue_stub, pr=pr, include_checks=include_checks, gh=gh)


def main() -> int:
    # Delta-only mode: if current export already exists, don't re-download.
    out_path = OUT_XLSX
    prev_path = PREVIOUS_WEEK_XLSX.strip() if isinstance(PREVIOUS_WEEK_XLSX, str) else ""
    if (
        DELTA_ONLY_IF_OUT_EXISTS
        and os.path.exists(out_path)
        and bool(prev_path)
        and os.path.exists(prev_path)
    ):
        try:
            current_df = _load_export_df(out_path)
            previous_df = _load_export_df(prev_path)
            changes_df, new_items_df = build_weekly_summary(current_df, previous_df)

            prs_df, issues_df = split_prs_issues(current_df)
            prs_df = reorder_columns_author_url_first(prs_df)
            issues_df = reorder_columns_author_url_first(issues_df)
            prs_updated_df = build_prs_updated_recent_view(prs_df, CLOSED_WITHIN_DAYS)
            weekly_pr_df = build_weekly_summary_view(
                build_weekly_updated_view(prs_df, WEEKLY_UPDATED_DAYS, item_type="pr")
            )
            report_issues_df = build_issues_report_view(issues_df)
            open_prs_df = build_open_prs_view(prs_df)

            with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
                prs_df.to_excel(writer, sheet_name=PRS_SHEET, index=False)
                if prs_updated_df.empty:
                    pd.DataFrame([{"note": f"No PRs updated in last {CLOSED_WITHIN_DAYS} days."}]).to_excel(
                        writer, sheet_name=PRS_UPDATED_SHEET, index=False
                    )
                else:
                    prs_updated_df.to_excel(writer, sheet_name=PRS_UPDATED_SHEET, index=False)
                if open_prs_df.empty:
                    pd.DataFrame([{"note": "No open PRs."}]).to_excel(writer, sheet_name=OPEN_PRS_SHEET, index=False)
                else:
                    open_prs_df.to_excel(writer, sheet_name=OPEN_PRS_SHEET, index=False)
                issues_df.to_excel(writer, sheet_name=ISSUES_SHEET, index=False)

                # Report tab (Issues only, requested columns)
                if report_issues_df.empty:
                    pd.DataFrame([{"note": "No issues found for given criteria."}]).to_excel(
                        writer, sheet_name=REPORT_SHEET, index=False
                    )
                else:
                    report_issues_df.to_excel(writer, sheet_name=REPORT_SHEET, index=False)

                # Weekly Summary (PRs only, last N days)
                if weekly_pr_df.empty:
                    pd.DataFrame([{"note": f"No PRs updated in last {WEEKLY_UPDATED_DAYS} days."}]).to_excel(
                        writer, sheet_name=WEEKLY_SUMMARY_SHEET, index=False
                    )
                else:
                    weekly_pr_df.to_excel(writer, sheet_name=WEEKLY_SUMMARY_SHEET, index=False)

                start_row = 0
                pd.DataFrame([{"section": "CHANGES (tracked: commits, comments, state, status)"}]).to_excel(
                    writer, sheet_name=WEEKLY_DELTA_SHEET, index=False, startrow=start_row
                )
                start_row += 2
                if not changes_df.empty:
                    changes_df.to_excel(writer, sheet_name=WEEKLY_DELTA_SHEET, index=False, startrow=start_row)
                    start_row += len(changes_df) + 3
                else:
                    pd.DataFrame([{"note": "No changes vs previous week."}]).to_excel(
                        writer, sheet_name=WEEKLY_DELTA_SHEET, index=False, startrow=start_row
                    )
                    start_row += 3

                pd.DataFrame([{"section": "NEW ITEMS"}]).to_excel(
                    writer, sheet_name=WEEKLY_DELTA_SHEET, index=False, startrow=start_row
                )
                start_row += 2
                if not new_items_df.empty:
                    new_items_df.to_excel(writer, sheet_name=WEEKLY_DELTA_SHEET, index=False, startrow=start_row)
                else:
                    pd.DataFrame([{"note": "No new items vs previous week."}]).to_excel(
                        writer, sheet_name=WEEKLY_DELTA_SHEET, index=False, startrow=start_row
                    )

            print(
                f"Delta-only mode: updated {out_path} (tabs: {PRS_SHEET}, {PRS_UPDATED_SHEET}, {OPEN_PRS_SHEET}, "
                f"{ISSUES_SHEET}, {REPORT_SHEET}, {WEEKLY_SUMMARY_SHEET}, {WEEKLY_DELTA_SHEET}) using {prev_path}."
            )
            return 0
        except Exception as e:
            print(f"WARNING: Delta-only mode failed ({e}); falling back to downloading.", file=sys.stderr)

    if not TOKEN:
        print(
            "ERROR: Missing GitHub token. Set env var GITHUB_TOKEN or paste it into TOKEN in the script.",
            file=sys.stderr,
        )
        return 2

    updated_since_dt = None
    if UPDATED_SINCE_DAYS is not None and int(UPDATED_SINCE_DAYS) > 0:
        updated_since_dt = _utc_now() - dt.timedelta(days=int(UPDATED_SINCE_DAYS))

    closed_since_dt = None
    if CLOSED_WITHIN_DAYS is not None and int(CLOSED_WITHIN_DAYS) > 0:
        closed_since_dt = _utc_now() - dt.timedelta(days=int(CLOSED_WITHIN_DAYS))

    gh = GitHubClient(token=TOKEN, api_base=API_BASE)

    # Preflight: avoid Search API 422 by removing inaccessible org/repos qualifiers.
    org_to_use = ORG
    if ORG:
        try:
            if not gh.get_org(ORG):
                print(
                    f"WARNING: org '{ORG}' not accessible with this token; ignoring ORG filter.",
                    file=sys.stderr,
                )
                org_to_use = None
        except Exception as e:
            print(f"WARNING: org preflight failed ({ORG}): {e}; ignoring ORG filter.", file=sys.stderr)
            org_to_use = None

    repos_to_use: List[str] = []
    if REPOS:
        for r in REPOS:
            try:
                if gh.get_repo(r):
                    repos_to_use.append(r)
                else:
                    print(
                        f"WARNING: repo '{r}' not accessible with this token; ignoring this repo filter.",
                        file=sys.stderr,
                    )
            except Exception as e:
                print(f"WARNING: repo preflight failed ({r}): {e}; ignoring this repo filter.", file=sys.stderr)

        if REPOS and not repos_to_use:
            print(
                "WARNING: none of the configured REPOS are accessible; running search without repo filters.",
                file=sys.stderr,
            )

    rows: List[Dict[str, Any]] = []
    seen: set[Tuple[str, int, str]] = set()  # (repo_full_name, number, item_type)
    hidden = {o.lower() for o in HIDE_ORGS if o}

    for user in USERS:
        for sub_state in _state_splits_for_all(STATE):
            print(f"INFO: collecting PRs for author={user} state={sub_state}", file=sys.stderr)
            # ---- PRs (Search) ----
            # For STATE="all", we want closed PR coverage based on UPDATED_SINCE_DAYS too,
            # otherwise older-but-relevant merged PRs can be missed.
            pr_updated_since = updated_since_dt if (sub_state == "open" or (STATE == "all" and sub_state == "closed")) else None
            pr_closed_since = closed_since_dt if (sub_state == "closed" and STATE != "all") else None

            q = build_search_query(
                author=user,
                state=sub_state,
                org=org_to_use,
                repos=repos_to_use,
                updated_since=pr_updated_since,
                closed_since=pr_closed_since,
                item_type="pr",
            )
            print(f"INFO: PR search q={q}", file=sys.stderr)

            search_failed = False
            search_rows_added = 0

            def _run_pr_search(query: str) -> None:
                nonlocal search_rows_added
                for issue in gh.search_prs(q=query, per_page=int(PER_PAGE)):
                    pr_api_url = (issue.get("pull_request") or {}).get("url")
                    if not pr_api_url:
                        continue
                    pr = gh.get_pr(pr_api_url)
                    if pr.get("draft"):
                        continue
                    html_url = issue.get("html_url") or pr.get("html_url") or ""
                    repo_ref = _repo_ref_from_html_url(html_url)
                    if repo_ref and repo_ref.owner.lower() in hidden:
                        continue
                    number = _pr_number_from_html_url(html_url) or pr.get("number")
                    if repo_ref and number:
                        key = (repo_ref.full_name, int(number), "pr")
                        if key in seen:
                            continue
                        seen.add(key)
                    rows.append(
                        extract_pr_row(
                            author=user,
                            issue=issue,
                            pr=pr,
                            include_checks=bool(INCLUDE_CHECKS),
                            gh=gh,
                        )
                    )
                    search_rows_added += 1
                    if search_rows_added % 50 == 0:
                        print(f"INFO: PRs added for {user} ({sub_state}) = {search_rows_added}", file=sys.stderr)

            try:
                _run_pr_search(q)
            except GitHubApiError as e:
                search_failed = True
                msg = str(e)
                if " 422 " in msg or "error 422" in msg:
                    fallback_q = build_search_query(
                        author=user,
                        state=sub_state,
                        org=None,
                        repos=[],
                        updated_since=pr_updated_since,
                        closed_since=pr_closed_since,
                        item_type="pr",
                    )
                    print(
                        f"WARNING: PR search query failed for user '{user}'. Retrying without ORG/REPOS filters.\n"
                        f"  failed_q: {q}\n"
                        f"  retry_q:  {fallback_q}",
                        file=sys.stderr,
                    )
                    _run_pr_search(fallback_q)
                else:
                    raise

            # ---- PRs (Fallback scan repos) ----
            if FALLBACK_SCAN_REPOS and repos_to_use and (search_failed or search_rows_added == 0):
                print(
                    f"INFO: fallback repo scan for {user} ({sub_state}); repos={len(repos_to_use)}",
                    file=sys.stderr,
                )
                for repo_name in repos_to_use:
                    owner, repo = repo_name.split("/", 1)
                    repo_ref = RepoRef(owner=owner, repo=repo)
                    if repo_ref.owner.lower() in hidden:
                        continue
                    cutoff_dt = None
                    if sub_state == "open" and updated_since_dt:
                        cutoff_dt = updated_since_dt
                    elif sub_state == "closed":
                        if STATE == "all" and updated_since_dt:
                            cutoff_dt = updated_since_dt
                        elif closed_since_dt:
                            cutoff_dt = closed_since_dt

                    for pr in gh.list_pulls(repo_ref, state=sub_state if sub_state in ("open", "closed") else "all"):
                        pr_user = (pr.get("user") or {}).get("login")
                        if pr_user != user:
                            continue
                        if pr.get("draft"):
                            continue

                        updated_at = _parse_iso8601(pr.get("updated_at")) if isinstance(pr.get("updated_at"), str) else None
                        if cutoff_dt and updated_at and updated_at < cutoff_dt:
                            # list_pulls is sorted by updated desc; once we're past cutoff, we can stop scanning.
                            break

                        if sub_state == "closed":
                            if STATE == "all" and updated_since_dt:
                                if updated_at and updated_at < updated_since_dt:
                                    continue
                            elif closed_since_dt:
                                closed_at = _parse_iso8601(pr.get("closed_at")) if isinstance(pr.get("closed_at"), str) else None
                                if closed_at and closed_at < closed_since_dt:
                                    # Not guaranteed to be sorted by closed_at, but keeps scan bounded.
                                    continue
                        elif sub_state == "open" and updated_since_dt:
                            if updated_at and updated_at < updated_since_dt:
                                continue

                        number = pr.get("number")
                        if number:
                            key = (repo_ref.full_name, int(number), "pr")
                            if key in seen:
                                continue
                            seen.add(key)
                        rows.append(
                            _extract_pr_row_from_pr_only(
                                author=user,
                                pr=pr,
                                include_checks=bool(INCLUDE_CHECKS),
                                gh=gh,
                            )
                        )

            # ---- Issues (Search) ----
            if INCLUDE_ISSUES:
                q = build_search_query(
                    author=user,
                    state=sub_state,
                    org=org_to_use,
                    repos=repos_to_use,
                    updated_since=pr_updated_since,
                    closed_since=pr_closed_since,
                    item_type="issue",
                )
                try:
                    for issue in gh.search_prs(q=q, per_page=int(PER_PAGE)):
                        # Ensure this is not a PR-like item
                        if (issue.get("pull_request") or {}).get("url"):
                            continue
                        html_url = issue.get("html_url") or ""
                        repo_ref = _repo_ref_from_html_url(html_url)
                        if repo_ref and repo_ref.owner.lower() in hidden:
                            continue
                        number = _pr_number_from_html_url(html_url) or issue.get("number")
                        if repo_ref and number:
                            key = (repo_ref.full_name, int(number), "issue")
                            if key in seen:
                                continue
                            seen.add(key)
                        rows.append(_extract_issue_row(author=user, issue=issue))
                except GitHubApiError as e:
                    msg = str(e)
                    if " 422 " in msg or "error 422" in msg:
                        fallback_q = build_search_query(
                            author=user,
                            state=sub_state,
                            org=None,
                            repos=[],
                            updated_since=pr_updated_since,
                            closed_since=pr_closed_since,
                            item_type="issue",
                        )
                        print(
                            f"WARNING: Issue search query failed for user '{user}'. Retrying without ORG/REPOS filters.\n"
                            f"  failed_q: {q}\n"
                            f"  retry_q:  {fallback_q}",
                            file=sys.stderr,
                        )
                        for issue in gh.search_prs(q=fallback_q, per_page=int(PER_PAGE)):
                            if (issue.get("pull_request") or {}).get("url"):
                                continue
                            html_url = issue.get("html_url") or ""
                            repo_ref = _repo_ref_from_html_url(html_url)
                            if repo_ref and repo_ref.owner.lower() in hidden:
                                continue
                            number = _pr_number_from_html_url(html_url) or issue.get("number")
                            if repo_ref and number:
                                key = (repo_ref.full_name, int(number), "issue")
                                if key in seen:
                                    continue
                                seen.add(key)
                            rows.append(_extract_issue_row(author=user, issue=issue))
                    else:
                        raise

    if not rows:
        print("No items found for given criteria.")
        return 0

    df = pd.DataFrame(rows)

    # Normalize & sort
    for col in ("created_at", "updated_at", "closed_at", "merged_at"):
        if col in df.columns:
            df[col] = df[col].apply(lambda x: _fmt_dt(_parse_iso8601(x) if isinstance(x, str) else None))

    # Keep stable column ordering, but enforce: author, url, then others.
    df = reorder_columns_author_url_first(df)

    # Ensure draft column is not exported (draft PRs are filtered out anyway).
    if "draft" in df.columns:
        df = df.drop(columns=["draft"])

    # Sort: per author, Issues first then PRs, then most recently updated.
    if "author" in df.columns and "item_type" in df.columns and "updated_at" in df.columns:
        updated_key = df["updated_at"].apply(lambda x: _parse_iso8601(x) if isinstance(x, str) else None)
        type_order = {"issue": 0, "pr": 1}
        type_key = df["item_type"].apply(lambda x: type_order.get(str(x).lower(), 99))
        df = df.assign(_updated_sort=updated_key, _type_sort=type_key)
        df = df.sort_values(by=["_type_sort", "author", "_updated_sort"], ascending=[True, True, False], kind="mergesort")
        df = df.drop(columns=["_updated_sort", "_type_sort"])
    elif "updated_at" in df.columns:
        # Fallback
        df = df.sort_values(by=["updated_at"], ascending=False, kind="mergesort")

    out_path = OUT_XLSX

    # Weekly "updated recently" view
    prs_df, issues_df = split_prs_issues(df)
    prs_df = reorder_columns_author_url_first(prs_df)
    issues_df = reorder_columns_author_url_first(issues_df)
    prs_updated_df = build_prs_updated_recent_view(prs_df, CLOSED_WITHIN_DAYS)
    weekly_pr_df = build_weekly_summary_view(build_weekly_updated_view(prs_df, WEEKLY_UPDATED_DAYS, item_type="pr"))
    report_issues_df = build_issues_report_view(issues_df)
    open_prs_df = build_open_prs_view(prs_df)

    # Optional week-over-week delta vs previous week's Excel.
    prev_path = PREVIOUS_WEEK_XLSX.strip() if isinstance(PREVIOUS_WEEK_XLSX, str) else ""
    prev_exists = bool(prev_path) and os.path.exists(prev_path)
    prev_df = None
    if prev_exists:
        try:
            prev_df = _load_export_df(prev_path)
        except Exception as e:
            print(f"WARNING: Failed to read previous week file '{prev_path}': {e}", file=sys.stderr)
            prev_exists = False

    with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
        prs_df.to_excel(writer, sheet_name=PRS_SHEET, index=False)
        if prs_updated_df.empty:
            pd.DataFrame([{"note": f"No PRs updated in last {CLOSED_WITHIN_DAYS} days."}]).to_excel(
                writer, sheet_name=PRS_UPDATED_SHEET, index=False
            )
        else:
            prs_updated_df.to_excel(writer, sheet_name=PRS_UPDATED_SHEET, index=False)
        if open_prs_df.empty:
            pd.DataFrame([{"note": "No open PRs."}]).to_excel(writer, sheet_name=OPEN_PRS_SHEET, index=False)
        else:
            open_prs_df.to_excel(writer, sheet_name=OPEN_PRS_SHEET, index=False)
        issues_df.to_excel(writer, sheet_name=ISSUES_SHEET, index=False)

        # Report tab (Issues only, requested columns)
        if report_issues_df.empty:
            pd.DataFrame([{"note": "No issues found for given criteria."}]).to_excel(writer, sheet_name=REPORT_SHEET, index=False)
        else:
            report_issues_df.to_excel(writer, sheet_name=REPORT_SHEET, index=False)

        # Weekly Summary tab (PRs only, updated last N days)
        if weekly_pr_df.empty:
            pd.DataFrame([{"note": f"No PRs updated in last {WEEKLY_UPDATED_DAYS} days."}]).to_excel(
                writer, sheet_name=WEEKLY_SUMMARY_SHEET, index=False
            )
        else:
            weekly_pr_df.to_excel(writer, sheet_name=WEEKLY_SUMMARY_SHEET, index=False)

        # Weekly Delta tab (only if previous week is available)
        if prev_exists and prev_df is not None:
            changes_df, new_items_df = build_weekly_summary(df, prev_df)
            start_row = 0
            pd.DataFrame([{"section": "CHANGES (tracked: commits, comments, state, status)"}]).to_excel(
                writer, sheet_name=WEEKLY_DELTA_SHEET, index=False, startrow=start_row
            )
            start_row += 2
            if not changes_df.empty:
                changes_df.to_excel(writer, sheet_name=WEEKLY_DELTA_SHEET, index=False, startrow=start_row)
                start_row += len(changes_df) + 3
            else:
                pd.DataFrame([{"note": "No changes vs previous week."}]).to_excel(
                    writer, sheet_name=WEEKLY_DELTA_SHEET, index=False, startrow=start_row
                )
                start_row += 3

            pd.DataFrame([{"section": "NEW ITEMS"}]).to_excel(
                writer, sheet_name=WEEKLY_DELTA_SHEET, index=False, startrow=start_row
            )
            start_row += 2
            if not new_items_df.empty:
                new_items_df.to_excel(writer, sheet_name=WEEKLY_DELTA_SHEET, index=False, startrow=start_row)
            else:
                pd.DataFrame([{"note": "No new items vs previous week."}]).to_excel(
                    writer, sheet_name=WEEKLY_DELTA_SHEET, index=False, startrow=start_row
                )

    if prev_exists:
        print(
            f"Wrote {len(df)} rows to {out_path} (tabs: {PRS_SHEET}, {PRS_UPDATED_SHEET}, {OPEN_PRS_SHEET}, "
            f"{ISSUES_SHEET}, {REPORT_SHEET}, {WEEKLY_SUMMARY_SHEET}, {WEEKLY_DELTA_SHEET})."
        )
    else:
        print(
            f"Wrote {len(df)} rows to {out_path} (tabs: {PRS_SHEET}, {PRS_UPDATED_SHEET}, {OPEN_PRS_SHEET}, "
            f"{ISSUES_SHEET}, {REPORT_SHEET}, {WEEKLY_SUMMARY_SHEET})."
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
