# Power BI Refresh Monitoring Pipeline

Automated extraction, storage, and analysis of Power BI semantic model refresh metadata using a Python-based pipeline running on Databricks Workflows.

## Architecture

```
Microsoft Entra ID       Databricks Notebook           Delta Lake
(Service Principal) ───► (MSAL + REST API calls) ───► (Unity Catalog Table)
                                    │
                         Databricks Workflow
                         (Daily @ 2:00 AM PST)
```

The pipeline:
1. Acquires an OAuth2 token via MSAL using Service Principal credentials stored in Databricks Secrets.
2. **Resolves the dataset registry** — explicit dataset entries plus optional workspace-level auto-discovery (`Get Datasets In Group`).
3. **Loads per-dataset watermarks** from the Delta table for incremental extraction.
4. Calls the Power BI REST API (`Get Refresh History In Group`) for each dataset **concurrently**, keeping only records newer than the watermark.
5. Transforms the JSON response — computes durations, extracts error codes, normalizes timestamps.
6. Upserts into a Delta Lake table using `MERGE INTO` on `request_id` to prevent duplicates.
7. Runs post-ingestion alert checks for failures, duration anomalies, and stale data.

### Scalability features

- **Workspace-level auto-discovery** — point the registry at a whole workspace and every dataset in it is found automatically; adding a workspace needs no per-report config.
- **Incremental extraction** — a per-dataset watermark (latest stored `start_time`) means each run only processes refreshes that happened since the last run, so call/processing volume stays flat as history grows.
- **Concurrent extraction** — a configurable thread pool (`api.max_concurrent_requests`) parallelizes per-dataset API calls; the built-in retry/`Retry-After` handling absorbs throttling (429s).
- **Partial-failure tolerance** — one failing dataset is logged and skipped without aborting the run.

## Project Structure

```
powerbi-refresh-monitor/
├── config/
│   ├── datasets.json          # Registry of monitored workspaces/datasets
│   ├── pipeline_config.json   # Pipeline settings (table name, API params, alerting)
│   └── workflow_job.json      # Databricks Workflow job definition
├── notebooks/
│   ├── run_pipeline.py        # Main pipeline notebook (daily execution)
│   ├── setup_table.py         # One-time Delta table creation
│   └── sample_queries.sql     # Analytical SQL queries
├── src/
│   ├── __init__.py
│   ├── auth.py                # OAuth2 token acquisition via MSAL
│   ├── api_client.py          # Power BI REST API client with retry logic
│   ├── transform.py           # JSON → structured row transformation
│   ├── delta_ops.py           # Delta table DDL, upsert, and watermark queries
│   ├── discovery.py           # Workspace-level dataset auto-discovery
│   └── alerts.py              # Post-ingestion alerting framework
├── tests/
│   ├── __init__.py
│   ├── test_auth.py
│   ├── test_api_client.py
│   ├── test_discovery.py
│   └── test_transform.py
├── requirements.txt
├── .gitignore
└── README.md
```

## Prerequisites

Before deploying the pipeline, complete these steps (Phase 1 from the proposal):

### 1. Entra ID App Registration
1. Register an application in [Microsoft Entra ID](https://portal.azure.com/#view/Microsoft_AAD_RegisteredApps/ApplicationsListBlade).
2. Generate a client secret (Certificates & secrets → New client secret).
3. Record the **Application (client) ID**, **Directory (tenant) ID**, and **Client Secret**.

### 2. Power BI Admin Configuration
1. In the [Power BI Admin Portal](https://app.powerbi.com/admin-portal/tenantSettings), enable **"Allow service principals to use Power BI APIs"** under Developer settings.
2. Scope the setting to a security group containing your Service Principal.
3. Add the Service Principal as a **Member** to each target workspace.

### 3. Databricks Secrets
```bash
# Create a secret scope
databricks secrets create-scope --scope powerbi-monitor

# Store credentials
databricks secrets put --scope powerbi-monitor --key tenant-id
databricks secrets put --scope powerbi-monitor --key client-id
databricks secrets put --scope powerbi-monitor --key client-secret
```

## Setup

### 1. Clone the Repo into Databricks
In your Databricks workspace:
- Go to **Repos → Add Repo** and clone this repository.
- Update the `REPO_ROOT` path in `notebooks/run_pipeline.py` and `notebooks/setup_table.py` to match your Databricks Repos path (e.g., `/Workspace/Repos/your-email/powerbi-refresh-monitor`).

### 2. Configure Datasets
Edit `config/datasets.json`. You can register datasets two ways (mix freely):

**a) Explicit datasets** — full control over names and the `is_critical` flag:
```json
{
  "datasets": [
    {
      "workspace_id": "f089354e-8366-4e18-aea3-4cb4a3a50b48",
      "workspace_name": "Finance Reports",
      "dataset_id": "cfafbeb1-8037-4d0c-896e-a46fb27ff229",
      "dataset_name": "Monthly Revenue Model",
      "is_critical": true,
      "is_active": true
    }
  ]
}
```

**b) Workspace-level auto-discovery** — monitor every dataset in a workspace without listing each one. Explicit entries take precedence; `exclude_datasets` lets you opt specific datasets out:
```json
{
  "workspaces": [
    {
      "workspace_id": "f089354e-8366-4e18-aea3-4cb4a3a50b48",
      "workspace_name": "Sales Analytics",
      "is_critical_default": false,
      "is_active": true,
      "exclude_datasets": ["<dataset-id-to-skip>"]
    }
  ]
}
```

### 3. Configure Pipeline Settings
Edit `config/pipeline_config.json`:
- Set `delta_table.catalog` and `delta_table.schema` to your Unity Catalog location.
- Optionally configure `alerting.teams_webhook_url` for Microsoft Teams notifications.
- Tune scalability knobs: `api.max_concurrent_requests` (parallel API calls) and `extraction.incremental` (set `false` to force a full re-extraction).

### 4. Create the Delta Table
Run the `notebooks/setup_table.py` notebook once to create the table.

### 5. Deploy the Workflow
Use `config/workflow_job.json` as the job definition:
- Via UI: Workflows → Create Job, then configure per the JSON.
- Via API: `POST /api/2.1/jobs/create` with the JSON payload.

Update the `notebook_path` and `email_notifications` fields before deploying.

## Local Development & Testing

```bash
# Install dependencies
pip install -r requirements.txt

# Run tests
pytest tests/ -v
```

Note: Tests for `auth` and `api_client` use mocks and do not require Databricks or Power BI access.

## Alerting

The pipeline includes three alert checks that run after each ingestion:

| Alert | Trigger | Default Threshold |
|---|---|---|
| **Refresh Failure** | Any dataset with `status = 'Failed'` | Immediate (critical datasets → Teams; non-critical → log) |
| **Duration Anomaly** | Duration > 1.5× the 7-day rolling average | Configurable in `pipeline_config.json` |
| **Stale Data** | No successful refresh for a critical dataset in 26 hours | Configurable in `pipeline_config.json` |

## Sample Queries

See `notebooks/sample_queries.sql` for ready-to-use analytical queries including:
- Latest refresh status per dataset
- Daily success/failure trends
- Duration trends and week-over-week comparisons
- Data freshness reports
- Concurrent refresh overlap detection

## Credential Rotation

Client secrets expire (default: 12 months). To rotate:
1. Generate a new secret in Entra ID.
2. Update the Databricks Secret: `databricks secrets put --scope powerbi-monitor --key client-secret`
3. Trigger a manual pipeline run to verify.
4. Delete the old secret from Entra ID.
