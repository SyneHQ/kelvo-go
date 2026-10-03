# Kelvo notebook lab

Try Kelvo with 15 standalone lessons and real public data. Each installs a checksum-verified preview binary, pinned Python packages and a reviewed helper. No database account, Go build or GPU is needed.

## Choose a starting point

| Lesson | What you will run | Open in Colab |
| --- | --- | --- |
| [Your first Kelvo query](01-first-query.ipynb) | First SQL query and exact Arrow results. | [Open](https://colab.research.google.com/github/SYNEHQ/kelvo-go/blob/cargo/notebooks/01-first-query.ipynb) |
| [Parameters, NULLs and exact types](02-parameters-nulls-and-types.ipynb) | Typed parameters, decimals and NULLs. | [Open](https://colab.research.google.com/github/SYNEHQ/kelvo-go/blob/cargo/notebooks/02-parameters-nulls-and-types.ipynb) |
| [Explore millions of taxi records](03-taxi-parquet-exploration.ipynb) | Summaries over one month of taxi trips. | [Open](https://colab.research.google.com/github/SYNEHQ/kelvo-go/blob/cargo/notebooks/03-taxi-parquet-exploration.ipynb) |
| [Join trips to taxi zones](04-joins-with-taxi-zones.ipynb) | Join Parquet trips to CSV zones. | [Open](https://colab.research.google.com/github/SYNEHQ/kelvo-go/blob/cargo/notebooks/04-joins-with-taxi-zones.ipynb) |
| [Build business metrics with CTEs](05-cte-business-metrics.ipynb) | Daily metrics with CTEs and exact money. | [Open](https://colab.research.google.com/github/SYNEHQ/kelvo-go/blob/cargo/notebooks/05-cte-business-metrics.ipynb) |
| [Windows, rankings and changing demand](06-window-functions-and-rankings.ipynb) | LAG, rankings and rolling windows. | [Open](https://colab.research.google.com/github/SYNEHQ/kelvo-go/blob/cargo/notebooks/06-window-functions-and-rankings.ipynb) |
| [Turn Arrow summaries into a dashboard](07-dashboard-from-arrow.ipynb) | Chart small Arrow summaries. | [Open](https://colab.research.google.com/github/SYNEHQ/kelvo-go/blob/cargo/notebooks/07-dashboard-from-arrow.ipynb) |
| [Make data-quality checks executable](08-data-quality-contracts.ipynb) | Check missing values and outliers. | [Open](https://colab.research.google.com/github/SYNEHQ/kelvo-go/blob/cargo/notebooks/08-data-quality-contracts.ipynb) |
| [Consume Arrow one batch at a time](09-bounded-arrow-consumption.ipynb) | Consume results batch by batch. | [Open](https://colab.research.google.com/github/SYNEHQ/kelvo-go/blob/cargo/notebooks/09-bounded-arrow-consumption.ipynb) |
| [Compare opt-in LZ4 results](10-opt-in-lz4-results.ipynb) | Compare LZ4 bytes and exact values. | [Open](https://colab.research.google.com/github/SYNEHQ/kelvo-go/blob/cargo/notebooks/10-opt-in-lz4-results.ipynb) |
| [Compose catalogs and reusable Parquet](11-catalog-federation-and-reusable-parquet.ipynb) | Join catalogs and reuse derived Parquet. | [Open](https://colab.research.google.com/github/SYNEHQ/kelvo-go/blob/cargo/notebooks/11-catalog-federation-and-reusable-parquet.ipynb) |
| [Refresh and verify an accelerated dataset](12-acceleration-refresh-and-verify.ipynb) | Refresh, verify and query a snapshot. | [Open](https://colab.research.google.com/github/SYNEHQ/kelvo-go/blob/cargo/notebooks/12-acceleration-refresh-and-verify.ipynb) |
| [Fail closed on schema drift and staleness](13-snapshot-schema-and-freshness.ipynb) | Reject schema drift and stale data. | [Open](https://colab.research.google.com/github/SYNEHQ/kelvo-go/blob/cargo/notebooks/13-snapshot-schema-and-freshness.ipynb) |
| [Run bounded concurrent queries](14-concurrent-independent-queries.ipynb) | Compare bounded serial/concurrent queries. | [Open](https://colab.research.google.com/github/SYNEHQ/kelvo-go/blob/cargo/notebooks/14-concurrent-independent-queries.ipynb) |
| [Query an authenticated HTTP gateway](15-authenticated-http-gateway.ipynb) | Submit, fetch and cancel through HTTP. | [Open](https://colab.research.google.com/github/SYNEHQ/kelvo-go/blob/cargo/notebooks/15-authenticated-http-gateway.ipynb) |

Start with 01 for a tiny download, 03–08 for analytics, 09–10 for Arrow, 11–13 for snapshots, or 15 for API integration. Lessons do not depend on one another.

## Run in Colab or another Jupyter environment

1. Open a lesson in a Python CPU runtime: glibc Linux x86-64, Python 3.11+.
2. Run cells from the top; setup verifies downloads before execution.
3. Inspect answer assertions and summaries. Save wanted artifacts before cleanup.

For another Jupyter service:

```sh
git clone --branch cargo https://github.com/SYNEHQ/kelvo-go.git
cd kelvo-go/notebooks
# Open a notebook in your existing Jupyter installation.
```

The runtime must allow HTTPS downloads and local subprocesses. Lesson 15 also needs an ephemeral loopback listener. Native macOS/Windows/ARM are outside the default binary path.

### Downloads and limits

| Data | Download/work |
| --- | --- |
| Palmer Penguins | Small CSV, 344 observations |
| NYC Taxi, January 2024 | 49,961,641-byte Parquet, 2,964,624 rows; optional zone CSV |
| Result exports | Usually summaries; record examples default to 50,000–100,000 rows |

The helper sets deadlines and memory/output budgets. Returned-row limits do not cap scans. DuckDB materializes before Arrow delivery; batch reading limits Python retention, not native RSS.

Cleanup removes private lab workspaces and stops their processes; verified download caches may remain. These tutorials check correctness, not production capacity. Colab hardware, caches and network vary.

## Public datasets and attribution

**Palmer Penguins:** Horst AM, Hill AP, Gorman KB (2020), *palmerpenguins: Palmer Archipelago (Antarctica) penguin data*. [Project](https://allisonhorst.github.io/palmerpenguins/) · [DOI](https://doi.org/10.5281/zenodo.3960218) · [CC0](https://github.com/allisonhorst/palmerpenguins/blob/8957207b78d6ccd1b4654a9dd9c9041b657478ab/DESCRIPTION). Downloads are pinned; lessons explicitly normalize `NA` where needed.

**NYC Taxi and Limousine Commission:** [trip records, zones and dictionaries](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) · [NYC Open Data terms](https://opendata.cityofnewyork.us/overview/#termsofuse). Read TLC's accuracy notice. January files can include timestamps outside January; lessons inspect/filter these. Dataset terms are separate from [Kelvo's code license](../LICENSE).

## Use your own data

Replace a trusted catalog entry or follow a [connector guide](../docs/usage.md#source-guides). Bind user values as typed parameters. Keep secrets in private references, never cells or saved outputs.

Lesson 15 can prompt for an existing HTTPS gateway token with `getpass`. Its default listener is authenticated loopback. Shared tenants need the [cluster model](../docs/cluster.md); do not expose a notebook `serve` catalog publicly.

## Continue building

[Usage](../docs/usage.md) · [Federation](../docs/federation.md) · [Adapter SDK](../docs/federation-adapters.md) · [Acceleration](../docs/acceleration.md) · [Schema policy](../docs/schema-evolution.md) · [Backups](../docs/snapshot-backup.md) · [Operations](../docs/operations.md) · [All docs](../docs/README.md)

Contributions should clear outputs, retain pinned downloads/resource limits and assert actual answers. Maintained by SYNEHQ.

## Validation and package updates

[Recorded acceptance](../docs/evidence/notebook-acceptance.json): all **15 notebooks / 73 code cells** passed public-bootstrap installation in fresh Linux Jupyter kernels. Notebook 01 also passed all five cells in managed Colab on 3 October 2026; the other 14 have Linux evidence only.

The [preview package](https://github.com/SyneHQ/kelvo-go/releases/tag/v0.1.0-preview.1) is pinned separately from `cargo`. Updates need a new immutable release, helper/notebook pins and a full curriculum run. Never replace assets beneath existing checksums.

Reproduce on the designated build host with a binary and verified dataset cache:

```sh
python3 -m pip install -r notebooks/requirements.txt nbformat==5.10.4 nbclient==0.10.2 ipykernel==6.30.1
python3 scripts/notebook_acceptance.py --binary bin/kelvo \
  --data-cache /path/to/verified-dataset-cache \
  --output-dir artifacts/notebook-new-run
```

Add `--public-bootstrap` to test fresh public installs in every kernel. Without it, the runner uses the supplied binary/local helper. [CI](../.github/workflows/ci.yml) repeats execution checks; neither mode proves tenant isolation or production throughput.
