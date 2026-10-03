# Browser edition

The same site as `web/`, with the service from `app/` ported to JavaScript so the whole thing runs in one page with no
server. SQL executes on SQLite compiled for the browser. It is meant for trying pipelines, demos and teaching.

Open `docs/index.html` through any static file server, or let GitHub Pages serve the `docs/` folder.

## What it does and does not do

Works: uploading CSV and JSON files, table sources, the pipeline builder and its SQL preview, runs with checks,
quarantine, the circuit breaker and the Gold publish gate, mappings, masking, SCD Type 1 and 2, schema changes, retries,
erasure, lineage, the tools, and schedules while the page is open. Work is saved in the browser's local storage.

Needs the deployed service instead: BigQuery, Cloud Storage, Azure Databricks and ADLS (a page cannot reach them), the
MCP endpoint for outside agents, the built-in agent, alert delivery, schedules while nobody has the page open, and the
zip download of the code export.

## Layout

| Path | What it is |
|---|---|
| `parts/2x-be-*.js` | The backend port. `a-core` is util, store and the SQLite engine; `b-medallion` is the SQL generator and the checks; `c-runner` is the run engine; `d-service` and `e-api` are the service functions, tools, routes and persistence. Each section names the Python file it mirrors. |
| `parts/82-live.js` | Only in this edition: file upload, the "What works here" panel, Start over. |
| other `parts/*` | The site from `web/parts`, with `api()` answered by the port instead of `fetch`, and wording adjusted. |
| `vendor/` | sql.js (MIT). |
| `build.sh` | Builds `docs/`. |
| `test/` | See below. |

The front-end parts are copies of `web/parts`. A change to the site has to be made in both places.

## Tests

```sh
sh browser/build.sh
node browser/test/steps.js && python3 browser/test/golden.py   # records how the Python service answers a 195-step scenario
node browser/test/parity.js                                    # the port must answer every step the same way
python3 browser/test/unit.py browser/test/vectors.json && node browser/test/unit.js   # hashing and type casting against Python
node browser/test/e2e_live.js                                  # the built page in a real browser (needs Playwright)
```

Known differences from the Python service, all deliberate:

- The message for a folder with no files names the landing area instead of a path on disk.
- A run that finds nothing new while earlier rows still wait in Bronze adds a hint to its log line.
- `try_cast` does not accept non-ASCII digits, prints year 1 as `0001`, and turns the number 4.0 into the text `4` where Python gives `4.0`.
