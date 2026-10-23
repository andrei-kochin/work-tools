# work-tools
Tools for the work routine automation

## Tools

### `fetch-team-prs.py`
A Python script to fetch GitHub Pull Requests and Issues authored by a specified list of team members and export the data into a comprehensive Excel spreadsheet.

**Key Features:**
- Generates multiple Excel sheets (All PRs, Open PRs, Issues, Weekly Summary, Weekly Delta, etc.).
- Supports a delta-only mode to compare current data against a previous week's export.
- Configurable via `config.yaml` (for GitHub `TOKEN` and `USERS` list) or environment variables.

**Usage:**
1. Ensure you have the required dependencies installed (`pandas`, `requests`, `openpyxl`, `pyyaml`).
2. Create a `config.yaml` file with your `TOKEN` and `USERS` (or set `GITHUB_TOKEN` and `GITHUB_USERS` environment variables).
3. Run the script:
   ```bash
   python fetch-team-prs.py
   ```

### `compare_docker_images.py`
Compare two container images and report which pull requests are in the difference. The default report classifies files as unmerged, already in a git baseline, or unpublished. Image layers and GitHub responses are cached next to the script, so a later run can pass `--offline`.

**Usage:**
```bash
python compare_docker_images.py IMAGE1 IMAGE2 --aiter v0.1.26 --out report.md
```

Python 3.11+ and the GitHub CLI (`gh`) are required. Docker is required only with `--engine docker`. No third-party Python packages.
