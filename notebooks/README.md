# Kelvo notebook lab

Fifteen runnable notebooks for trying Kelvo with real public datasets. Open any
lesson in Google Colab, or clone this repository and run it in Linux x86-64
Jupyter. Every analytical SQL query runs through the Kelvo executable or its
authenticated HTTP API. No database credentials are needed for the default path.

The notebooks install a checksum-verified preview binary, pinned Python readers,
and a reviewed helper. They do not compile Go or run a separate DuckDB Python
query engine. Use a CPU runtime; a GPU does not accelerate these examples.

## Choose a starting point

| Lesson | What you will run | Open in Colab |
| --- | --- | --- |
| [Your first Kelvo query](01-first-query.ipynb) | Register an open CSV, run SQL, inspect typed Arrow results, and validate the result against the original file. | [Open](https://colab.research.google.com/github/SYNEHQ/kelvo-go/blob/cargo/notebooks/01-first-query.ipynb) |
| [Parameters, NULLs and exact types](02-parameters-nulls-and-types.ipynb) | Bind user values without SQL interpolation and preserve integers, decimals and missing observations. | [Open](https://colab.research.google.com/github/SYNEHQ/kelvo-go/blob/cargo/notebooks/02-parameters-nulls-and-types.ipynb) |
| [Explore millions of taxi records](03-taxi-parquet-exploration.ipynb) | Query a real month of NYC trips while returning small, inspectable summaries. | [Open](https://colab.research.google.com/github/SYNEHQ/kelvo-go/blob/cargo/notebooks/03-taxi-parquet-exploration.ipynb) |
| [Join trips to taxi zones](04-joins-with-taxi-zones.ipynb) | Combine a Parquet fact table with a CSV dimension and prove the join does not multiply or discard trips. | [Open](https://colab.research.google.com/github/SYNEHQ/kelvo-go/blob/cargo/notebooks/04-joins-with-taxi-zones.ipynb) |
| [Build business metrics with CTEs](05-cte-business-metrics.ipynb) | Create an auditable daily operating report with explicit quality filters and exact money totals. | [Open](https://colab.research.google.com/github/SYNEHQ/kelvo-go/blob/cargo/notebooks/05-cte-business-metrics.ipynb) |
| [Windows, rankings and changing demand](06-window-functions-and-rankings.ipynb) | Use LAG, running sums and rolling windows on a daily aggregate, then verify the window boundaries. | [Open](https://colab.research.google.com/github/SYNEHQ/kelvo-go/blob/cargo/notebooks/06-window-functions-and-rankings.ipynb) |
| [Turn Arrow summaries into a dashboard](07-dashboard-from-arrow.ipynb) | Build a small charting workflow without moving millions of rows into pandas. | [Open](https://colab.research.google.com/github/SYNEHQ/kelvo-go/blob/cargo/notebooks/07-dashboard-from-arrow.ipynb) |
| [Make data-quality checks executable](08-data-quality-contracts.ipynb) | Measure missing values and outliers, define an eligible population, and prevent a misleading report. | [Open](https://colab.research.google.com/github/SYNEHQ/kelvo-go/blob/cargo/notebooks/08-data-quality-contracts.ipynb) |
| [Consume Arrow one batch at a time](09-bounded-arrow-consumption.ipynb) | Export a bounded excerpt and process each Arrow record batch without building one large pandas table. | [Open](https://colab.research.google.com/github/SYNEHQ/kelvo-go/blob/cargo/notebooks/09-bounded-arrow-consumption.ipynb) |
| [Compare opt-in LZ4 results](10-opt-in-lz4-results.ipynb) | Measure Arrow output bytes and verify that compression preserves every value and type. | [Open](https://colab.research.google.com/github/SYNEHQ/kelvo-go/blob/cargo/notebooks/10-opt-in-lz4-results.ipynb) |
| [Compose catalogs and reusable Parquet](11-catalog-federation-and-reusable-parquet.ipynb) | Join registered files, save a derived relation, and reuse it through the same Kelvo boundary. | [Open](https://colab.research.google.com/github/SYNEHQ/kelvo-go/blob/cargo/notebooks/11-catalog-federation-and-reusable-parquet.ipynb) |
| [Refresh and verify an accelerated dataset](12-acceleration-refresh-and-verify.ipynb) | Publish a managed Parquet snapshot, query its alias and prove reads no longer require the source file. | [Open](https://colab.research.google.com/github/SYNEHQ/kelvo-go/blob/cargo/notebooks/12-acceleration-refresh-and-verify.ipynb) |
| [Fail closed on schema drift and staleness](13-snapshot-schema-and-freshness.ipynb) | Test rejection paths against real source observations without overwriting a valid generation. | [Open](https://colab.research.google.com/github/SYNEHQ/kelvo-go/blob/cargo/notebooks/13-snapshot-schema-and-freshness.ipynb) |
| [Run bounded concurrent queries](14-concurrent-independent-queries.ipynb) | Compare serial and concurrent execution of independent real-data queries while reconciling every result. | [Open](https://colab.research.google.com/github/SYNEHQ/kelvo-go/blob/cargo/notebooks/14-concurrent-independent-queries.ipynb) |
| [Query an authenticated HTTP gateway](15-authenticated-http-gateway.ipynb) | Run the real local API lifecycle, then optionally connect the same notebook to your own HTTPS gateway. | [Open](https://colab.research.google.com/github/SYNEHQ/kelvo-go/blob/cargo/notebooks/15-authenticated-http-gateway.ipynb) |

Start with **01** for a tiny download, **03–08** for analytical SQL, **09–10** for
Arrow delivery, **11–13** for reusable datasets and managed snapshots, or **15**
for application integration. Every notebook includes its own setup and cleanup;
none depends on running an earlier lesson.

## Run in Colab or another Jupyter environment

1. Open a notebook and choose a Python CPU runtime. The initial release targets
   glibc-compatible Linux x86-64 and Python 3.11 or newer; native macOS/Windows or ARM runtimes are
   not the default binary-install path.
2. Run all cells from the top. The first cell installs pinned Python packages and
   verifies the helper and binary before executing them.
3. Inspect the assertions and small Arrow summaries, then save any chart or result
   you want to keep before running the cleanup cell.

For another hosted Jupyter service, clone the repository and open the `.ipynb`
file. A Python process must be allowed to download HTTPS files and launch a local
executable. Notebook 15 also opens an ephemeral loopback listener; services that
forbid subprocesses or local listeners cannot run every lesson. No public port,
Docker daemon, GPU, cloud account or paid database is required.

```sh
git clone --branch cargo https://github.com/SYNEHQ/kelvo-go.git
cd kelvo-go/notebooks
# Open a notebook in your existing Jupyter installation.
```

### Downloads and limits

- Lessons using Palmer Penguins read 344 observations from a small CSV.
- Taxi lessons download the original January 2024 NYC yellow-taxi Parquet file
  (49,961,641 bytes, 2,964,624 rows) and sometimes a small zone lookup.
  Most queries return a few dozen summary rows. Record exports default to
  50,000–100,000 rows; larger exports are explicitly opt-in.
- A returned-row limit does not cap scan work. The helper sets bounded query
  memory, deadlines, output rows and output bytes. DuckDB native allocations and
  worker RSS still require deployment limits.
- The pinned DuckDB driver materializes execution before Arrow delivery. Batch
  consumption bounds retained Python batches, not native query-execution memory.
- Cleanup removes the lab's private workspace and stops its processes. Save
  desired artifacts first. Reusable verified download caches can remain.

These are correctness-checked tutorials, not production capacity or performance
claims. Colab hardware, page-cache warmth and network speed vary. The LZ4 and
concurrency notebooks report their own measurements without asserting a speedup.
See the [published benchmark methodology](../docs/benchmarking.md) before making
comparisons.

## Public datasets and attribution

**Palmer Penguins:** Horst AM, Hill AP, Gorman KB (2020), *palmerpenguins: Palmer
Archipelago (Antarctica) penguin data*.
[Project](https://allisonhorst.github.io/palmerpenguins/) ·
[DOI](https://doi.org/10.5281/zenodo.3960218) ·
[CC0 license](https://github.com/allisonhorst/palmerpenguins/blob/8957207b78d6ccd1b4654a9dd9c9041b657478ab/DESCRIPTION).
The helper pins and verifies the original CSV. The missing-value literal `NA` is
normalized explicitly when a lesson requires numeric observations.

**NYC Taxi and Limousine Commission:**
[trip records, taxi-zone lookup and data dictionaries](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page).
Read the [NYC Open Data terms](https://opendata.cityofnewyork.us/overview/#termsofuse)
and TLC's accuracy notice. January file membership is not a guarantee that every
pickup timestamp falls in January. The notebooks inspect and filter these
exceptions explicitly. Dataset rights and terms are separate from Kelvo's
[Apache-2.0 code license](../LICENSE).

## Use your own data

Replace a catalog entry with a local CSV/Parquet file, or follow a source-specific
[connector guide](../docs/usage.md#source-guides). Keep identifiers in a trusted
catalog and bind user-supplied values as typed parameters. Credentials belong in
private environment/secret references, never in notebook cells or saved outputs.

Notebook 15 optionally uses your own HTTPS gateway and prompts for its token with
`getpass`. Its default path starts an authenticated loopback gateway over public
data. A local `serve` token covers one configured catalog; shared tenants require
the [cluster security and deployment model](../docs/cluster.md), not a notebook
listener exposed to the Internet.

## Continue building

| Next step | Guide |
| --- | --- |
| Register sources and use the API | [Usage](../docs/usage.md) |
| Join live database tables | [Native federation](../docs/federation.md) |
| Build a custom native adapter | [Federation adapter interface](../docs/federation-adapters.md) |
| Refresh persistent analytical copies | [Acceleration](../docs/acceleration.md) |
| Evolve and recover snapshots | [Schema evolution](../docs/schema-evolution.md) and [backups](../docs/snapshot-backup.md) |
| Operate worker pools | [Cluster](../docs/cluster.md) and [operations](../docs/operations.md) |
| See implementation and validation limits | [Production roadmap](../docs/production-roadmap.md) |
| Find every guide | [Documentation index](../docs/README.md) |

Notebook source and helpers are maintained by SYNEHQ. Contributions should keep
outputs cleared, preserve pinned downloads and explicit resource limits, use
publicly reproducible data, and include assertions that check the actual answer.

## Validation and package updates

The [frozen curriculum record](../docs/evidence/notebook-acceptance.json) checks
all 15 notebooks and all 73 code cells through public package installation in fresh Linux Jupyter kernels, with
source/binary/helper hashes and exact dataset provenance. The checked-in copies
keep outputs cleared. Managed Colab hardware and runtime availability remain
controlled by Google; the notebooks require a Python CPU runtime with subprocess
and HTTPS access.

The [notebook preview package](https://github.com/SyneHQ/kelvo-go/releases/tag/v0.1.0-preview.1)
is built from a recorded commit, separate from ongoing development on `cargo`.
The helper pins the archive checksum in code and checks it against the release
manifest. Updating it requires a new immutable package, helper commit and notebook
references followed by the complete curriculum gate. Release assets must not be
replaced underneath existing notebook pins.

To reproduce validation with an existing binary and the verified public datasets:

```sh
python3 -m pip install -r notebooks/requirements.txt nbformat==5.10.4 nbclient==0.10.2 ipykernel==6.30.1
python3 scripts/notebook_acceptance.py --binary bin/kelvo \
  --data-cache /path/to/verified-dataset-cache \
  --output-dir artifacts/notebook-new-run
```

The regular gate uses the operator's binary and local helper, and records that
boundary. It proves notebook execution and answers, not tenant isolation or
production throughput. [CI](../.github/workflows/ci.yml) repeats this gate on
changes; the package's checksum and the runner's results are separate checks.

Add `--public-bootstrap` to execute dependency installation, the pinned helper
download and a fresh verified release install in every kernel. The published
record uses that mode. It certifies the public notebook setup on the named Linux
runtime; it does not claim execution inside a managed Google Colab session.
