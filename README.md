# Medallion Control Plane

A small service with a web site that builds and runs Bronze, Silver and Gold pipelines on your own
warehouse. Nothing in it is sample or simulated data: every table, row count, run, log line and check
result comes from the warehouse or from the runs this service executed.

## What it does

- **Connections.** BigQuery (files from Cloud Storage), Azure Databricks through a SQL warehouse (files from
  ADLS or any path the warehouse can read), and a local SQLite engine for trying things with no cloud at all.
- **Pipeline builder.** Describe a dataset once: where it comes from, its columns, the Gold model, a schedule.
  It can read the column names and types from the real file or table. You see the exact SQL before deploying.
- **Runs.** Bronze lands the source as text with lineage columns and loads only new files or rows. Silver casts,
  runs the row checks, quarantines bad rows with the reason, removes duplicates and upserts (or keeps history,
  SCD Type 2). Gold is rebuilt and, with the publish gate on, replaces the old version only after its checks pass.
- **Controls.** Per pipeline: which checks run, the pass rate each must reach, and what a failure does (stop,
  quarantine, warn); what happens on a new column or an empty source; a limit that stops the run when too much
  would be quarantined; retries; who gets alerted.
- **Operations.** Scheduler (cron, UTC), retry from the failed task, full reload, live logs, freshness alerts,
  catalog and lineage, table previews, quarantine browser, audit log, erasure requests, access grants.
- **MCP and agent.** The service is an MCP server at `/mcp`, so an agent can inspect and operate pipelines.
  Tools that change something wait for a person's approval. A built-in agent runs when `ANTHROPIC_API_KEY` is set.

## What has been tested, and what has not

| Part | Status |
|---|---|
| Run engine, checks, controls, quarantine, SCD1 and SCD2, retries, gate | Tested end to end on the local engine (`python -m tests.test_engine_local`, 23 checks) |
| HTTP API, token protection, tools, approvals, MCP endpoint | Tested against a live server (`python -m tests.test_api`, 31 checks) and with an MCP client library |
| The web site | Driven in a real browser against a live server (`node tests/e2e_browser.js`, 26 checks) |
| Cloud engines: requests, polling, result parsing, file listing | Tested against fake BigQuery and Databricks servers (`python -m tests.test_cloud_plumbing`, 18 checks) |
| **BigQuery engine: the SQL itself** | Written for the BigQuery dialect and reviewed statement by statement. **Not yet run against a real project.** |
| **Azure Databricks engine: the SQL itself** | Written for Databricks SQL and reviewed. **Not yet run against a real workspace.** |
| `deploy.sh` | Standard gcloud commands. **Not yet run.** |
| Built-in agent | The tool loop follows the Claude Messages API. **Not yet run with a real key.** |
| Spark, Airflow and Databricks Workflows export | Generated templates. This service does not run them. |

Expect to fix small things on the first run against BigQuery or Databricks. `python tests/print_cloud_sql.py`
prints every statement shape for each dialect, which is the quickest way to see what was sent.

## Run it locally (no cloud needed)

Python 3.10 or newer.

```bash
pip install -r requirements.txt
APP_TOKEN=choose-a-token uvicorn app.main:app --port 8080
```

Open http://localhost:8080 and enter the token. Then:

1. **Connections → New connection → Local SQLite lake**, name `local_lake`, folder `./data/lake`.
2. **New connection → Local folder**, name `landing`, folder `./samples/landing`.
3. **Pipeline builder**: dataset `vendor_returns`, folder `vendor_returns`, select **Read columns from the source**,
   fill in the Gold step (date column `return_date`, group by `reason, channel`, measure `SUM(refund_amount) AS refund_total`),
   then **Deploy and run now**.

The sample file contains a missing key, an amount written as `12,90`, a bad date and a duplicate on purpose.
The first run stops on the missing key. Open **Controls**, set that check to quarantine, raise the quarantine
limit, and retry from the failed task.

## Deploy to Cloud Run (from Cloud Shell, nothing to install)

1. Open https://shell.cloud.google.com, select your sandbox project (`gcloud config set project <id>`).
2. Upload this zip with the Cloud Shell **Upload** button, then `unzip medallion-control-plane.zip && cd medallion-control-plane`.
3. `./deploy.sh`. It prints the address and the access token.

What the script sets up: a service account with BigQuery Data Editor, BigQuery Job User, Storage Object Viewer and
Secret Manager Secret Accessor; a bucket that holds the control plane's own state; and one always-on Cloud Run
instance with CPU outside requests, because runs and the scheduler work in the background. An always-on instance
has a monthly cost. Set `--min-instances 0` on the service when you are not using it.

If your organisation blocks public Cloud Run services, remove `--allow-unauthenticated` and reach the service
through `gcloud run services proxy`.

### First pipeline on BigQuery

```bash
gcloud storage buckets create gs://$GOOGLE_CLOUD_PROJECT-landing --location us-central1
gcloud storage cp samples/landing/vendor_returns/*.csv gs://$GOOGLE_CLOUD_PROJECT-landing/vendor_returns/
```

On the site: add a **BigQuery** connection (leave the project empty to use the service's project, set the location
to match your bucket, for example `us-central1`), add a **Cloud Storage bucket** connection with
`gs://<project>-landing`, then use the builder as above. Datasets `bronze`, `silver` and `gold` are created for you
(set `SCHEMA_PREFIX` to keep them apart from existing datasets).

### Azure Databricks

1. In Databricks create a SQL warehouse and note its id, and choose a Unity Catalog catalog the service may write to.
2. Create a token for a service principal (or yourself) with `USE CATALOG`, `CREATE SCHEMA` and `CREATE TABLE` on that catalog.
3. Store the token where the service can read it, as a secret in Google Secret Manager:
   `printf '%s' "<token>" | gcloud secrets create DATABRICKS_TOKEN --data-file=-`
   (or as an environment variable of the same name).
4. On the site: add an **Azure Databricks** connection with the workspace URL, the warehouse id, the catalog, and
   `DATABRICKS_TOKEN` as the secret reference. Add an **ADLS or cloud storage path** connection for the files.
   That path must be an external location or volume the warehouse is allowed to read.

## Settings

| Variable | Meaning |
|---|---|
| `APP_TOKEN` | Access token for the site, the API and `/mcp`. Without it anyone who reaches the address has full control. |
| `STATE_URI` | Where the control plane keeps its own state: a local path, or `gs://bucket/state.json`. |
| `SCHEMA_PREFIX` | Prefix for the three schemas or datasets, for example `mcp_`. |
| `ENVIRONMENT` | Label shown on the site (`dev`, `test`, `prod`). |
| `SCHEDULER` | `off` disables the built-in scheduler. Then call `POST /api/tick` once a minute from Cloud Scheduler. |
| `ALERT_WEBHOOK_URL`, `PAGER_WEBHOOK_URL` | Where chat and pager alerts are posted. |
| `SMTP_HOST`, `SMTP_PORT`, `SMTP_USER`, `SMTP_PASSWORD`, `SMTP_FROM`, `ALERT_EMAIL_TO` | Email alerts. |
| `ANTHROPIC_API_KEY`, `AGENT_MODEL` | Switch the built-in agent on and choose its model. |
| `RETRY_BASE_SECONDS` | First wait before a retry (doubles each time). Default 5. |

Secrets are never stored by the service. A connection keeps only the name of a secret, and the value is read from
an environment variable of that name or from Secret Manager when it is needed.

## Connect an MCP client

```json
{ "mcpServers": { "medallion-control-plane": { "type": "http", "url": "https://<your-service>/mcp",
    "headers": { "Authorization": "Bearer <APP_TOKEN>" } } } }
```

## Limits of this version

- Sources are files (CSV, JSON lines, Parquet, Avro; the local engine reads CSV and JSON) and tables already in
  the warehouse. Databases over JDBC and streams are not loaded by this service.
- Runs execute SQL on the warehouse. There is no Spark job submission. The Spark and orchestration files under
  **Code export** are for teams that want to take the logic elsewhere.
- CSV files in one folder are expected to share a header. On BigQuery the header of the first new file is used.
- One instance only: state is a single JSON document and runs are threads in the service.
- Quarantined rows can be inspected and discarded. Replaying them into Silver is done by fixing the source or the
  mapping and running a full reload.
- Restoring an earlier table version is left to the warehouse (BigQuery time travel, Delta `RESTORE`).

## Layout

```
app/            the service
  engines/      local SQLite, BigQuery, Databricks
  medallion.py  pipeline definition -> SQL for each layer
  quality.py    checks -> SQL
  runner.py     run engine, controls, scheduler
  service.py    everything the API offers
  tools.py      tools shared by MCP, the agent and the site
  static/       the built site (one file)
web/            source of the site and its build script (web/build.sh)
samples/        a small file with deliberate data problems
tests/          engine, API and browser tests
```
