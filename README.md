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
