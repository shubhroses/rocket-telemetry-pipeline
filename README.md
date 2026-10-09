# Rocket engine telemetry pipeline

An end-to-end ELT example built on synthetic rocket engine telemetry. A Python generator emits sensor readings with deliberate data-quality faults, and a cleaning script validates and de-duplicates them. In the setup the project was written for, Airbyte Cloud loads the cleaned file into Amazon Redshift Serverless, dbt models it into a fact table and two reporting marts, and a Streamlit dashboard reads the results.

Every reading is produced by `python/generate_telemetry.py`; there is no real telemetry here, and the engines and the manufacturer name used in the models are made up. The generator and the cleaner need only the Python standard library. The warehouse stages need a Redshift database and an Airbyte connection that are not part of this repository (see [Limitations](#limitations)).

## Pipeline

```
python/generate_telemetry.py     JSON Lines, faults injected
          |
python/ingest_and_clean.py       validated, de-duplicated CSV
          |
git push                         Airbyte Cloud reads the committed CSV from GitHub
          |
Redshift Serverless              telemetry_raw.telemetry_data
          |
dbt (dbt/telemetry_analytics)    stg_telemetry_readings            view
                                   -> fact_telemetry_readings      table
                                        -> engine_performance_summary   table
                                        -> daily_anomaly_trends         table
          |
python/streamlit_dashboard.py    reads the fact table and both marts
```

## Repository layout

| Path | Contents |
|---|---|
| `python/generate_telemetry.py` | Synthetic telemetry generator (standard library only) |
| `python/ingest_and_clean.py` | Validation, correction and de-duplication (standard library only) |
| `python/streamlit_dashboard.py` | Dashboard over the dbt outputs |
| `dbt/telemetry_analytics/` | dbt project: staging, core and marts models, schema tests |
| `dbt/profiles.yml.example` | dbt profile template that reads host, user and password from environment variables |
| `sql/star_schema_design.sql` | Redshift DDL for the fact, dimension and bridge tables |
| `config/redshift_connection_template.json` | Template for the dashboard's connection file |
| `config/airbyte_config.json` | Placeholder Airbyte API settings; no script reads it |
| `data/` | Output of one sample run (raw and cleaned) |
| `requirements-stable.txt` | Pinned dbt, Streamlit, pandas, Plotly and psycopg2 versions |
| `tests/test_pipeline.py` | pytest suite for the generator and the cleaner |
| `tests/test_dashboard.py` | pytest checks for the dashboard: static ones that run everywhere, and render ones that need the dashboard's dependencies |
| `requirements-dev.txt`, `pytest.ini`, `ruff.toml` | Test and lint dependencies (pytest, ruff) and their configuration |
| `.github/workflows/ci.yml` | GitHub Actions workflow: pytest, then `dbt deps` and `dbt parse` |

## What each stage does

### Generator

- Simulates five engines, `ENG-001` to `ENG-005`. Each has a fixed performance factor (0.75 to 0.92) that sets its typical chamber pressure (psi), fuel flow (kg/s) and temperature (°F). Gaussian noise is added to every reading.
- Timestamps start at the current time and advance by 1 to 5 seconds per reading.
- Injects faults: about 3% of readings lose one or two sensor fields, about 5% get one out-of-range value (for example a negative pressure), and about 4% are emitted twice. Between 0.16% and 0.36% of readings, depending on the engine, simulate a critical failure (zero fuel flow, pressure spike, thermal runaway or unstable combustion).
- Writes JSON Lines to stdout and progress messages to stderr.

### Cleaner

- Drops records that lack `timestamp` or `engine_id`, have an unparseable timestamp or a non-numeric measurement, or report a temperature below -273.15.
- Replaces a negative chamber pressure with its absolute value and a fuel flow of exactly zero with 0.1.
- Removes duplicates on (`timestamp`, `engine_id`), keeping the first occurrence.
- Keeps readings with missing measurements and writes them as empty cells. The dbt staging model later drops rows in which a measurement is null.
- Logs every correction and a run summary (processed, dropped, corrected, duplicates removed) to stderr and to `errors.log` in the working directory.

### dbt models

The source is `dev.telemetry_raw.telemetry_data`, the table Airbyte writes, including Airbyte's `_airbyte_raw_id`, `_airbyte_extracted_at` and `_airbyte_generation_id` columns.

| Model | Type | What it adds |
|---|---|---|
| `stg_telemetry_readings` | view | Type casts; drops rows with a missing measurement; `fuel_efficiency_ratio` (fuel flow / chamber pressure); a 0-100 `performance_score` (40 points for pressure within 120-350 psi, 30 for fuel flow within 30-180 kg/s, 30 for temperature within 1800-4200 °F); `is_anomaly` and `anomaly_type` when pressure is outside 80-400 psi, fuel flow outside 15-220 kg/s or temperature outside 1200-4500 °F |
| `fact_telemetry_readings` | table | `reading_key` surrogate key; hourly `time_key` (YYYYMMDDHH); keys looked up from `telemetry_clean.dim_engines` and `telemetry_clean.dim_telemetry_metrics` (-1 when there is no match); `is_out_of_normal_range` from the ranges stored in the metrics dimension; a per-reading `health_status` from EXCELLENT to CRITICAL based on the performance score. Runs `ANALYZE` as a post-hook |
| `engine_performance_summary` | table | One row per engine: averages, minimums, maximums and standard deviations, anomaly counts and rate, rankings, and a health status from the anomaly rate (up to 5% EXCELLENT, up to 15% GOOD, up to 30% FAIR, otherwise NEEDS_ATTENTION) |
| `daily_anomaly_trends` | table | One row per day: anomaly counts by type, performance statistics, 7-day moving averages, day-over-day and week-over-week changes, trend labels, and an `alert_flag` (anomaly rate above 25%, any CRITICAL reading, or a day-over-day rise of more than 10 points) |

`models/schema.yml` and `models/staging/sources.yml` declare 44 generic tests: 33 `not_null`, 5 `unique`, 4 `accepted_values` and 2 `dbt_utils.unique_combination_of_columns`. `store_failures` is enabled in `dbt_project.yml`, so the rows that fail a test are saved in the warehouse under a `test_failures` schema suffix.

### Dashboard

`python/streamlit_dashboard.py` queries `telemetry_clean_marts.engine_performance_summary`, `telemetry_clean_marts.daily_anomaly_trends` (latest 7 days) and `telemetry_clean_core.fact_telemetry_readings` (latest 50 readings). It shows headline metrics, a performance gauge per engine, the engine summary table, time series of pressure, fuel flow and temperature by engine, an alert banner when the latest day has an `alert_flag`, and a table of recent anomalous readings.

## Running it

### 1. Generate and clean data

This step needs only Python 3 and no cloud account.

```bash
python3 python/generate_telemetry.py 1000 > data/telemetry_raw.csv
python3 python/ingest_and_clean.py --input data/telemetry_raw.csv --output data/telemetry_clean.csv
```

`data/telemetry_raw.csv` holds JSON Lines despite its extension. The two scripts can also be piped, in which case the cleaner writes to `data/telemetry_clean.csv` by default:

```bash
python3 python/generate_telemetry.py 1000 | python3 python/ingest_and_clean.py
```

Both forms overwrite sample files that are tracked in `data/` (see [Sample data](#sample-data)); `git restore data/` brings the committed sample back. One test compares those files with the figures quoted in that section, so the suite fails while `data/` holds a different run.

### 2. Warehouse prerequisites

The remaining steps expect:

- An Amazon Redshift database named `dev` with the schemas `telemetry_raw` and `telemetry_clean`. The models and DDL use Redshift SQL (`GETDATE()`, `ANALYZE`, `DISTKEY`, `SORTKEY`).
- An Airbyte connection that loads the cleaned CSV into `telemetry_raw.telemetry_data`. The connection was set up in the Airbyte Cloud UI and is not defined in this repository.
- The dimension tables `telemetry_clean.dim_engines` and `telemetry_clean.dim_telemetry_metrics`, which the fact model reads directly. Their DDL is in `sql/star_schema_design.sql`.

### 3. Run dbt

```bash
python3.11 -m venv .venv
source .venv/bin/activate
pip install -r requirements-stable.txt

cp dbt/profiles.yml.example dbt/profiles.yml
export REDSHIFT_HOST=<workgroup>.<account-id>.<region>.redshift-serverless.amazonaws.com
export REDSHIFT_USER=<user>
export REDSHIFT_PASSWORD=<password>

cd dbt/telemetry_analytics
dbt deps
dbt run --profiles-dir ..
dbt test --profiles-dir ..
```

The pinned versions were released between December 2022 and September 2024. The pinned pandas, grpcio and psycopg2-binary releases have no wheels for Python 3.12 or later, so use Python 3.11 or earlier. The install was checked on Python 3.10. `dbt deps` installs `dbt_utils` 1.3.0, which two of the tests use.

dbt needs a profile named `telemetry_analytics` (the `profile` set in `dbt_project.yml`). `dbt/profiles.yml.example` is that profile with the host, user and password read from the environment variables `REDSHIFT_HOST`, `REDSHIFT_USER` and `REDSHIFT_PASSWORD`, so the copy holds no connection details. `dbt/profiles.yml` is git-ignored. dbt does not look in `dbt/` by default, hence `--profiles-dir ..`; copying the example to `~/.dbt/profiles.yml` works without the flag.

The example sets `schema: telemetry_clean`, and that value should stay. With dbt's default schema naming the models are then built in `telemetry_clean_staging`, `telemetry_clean_core` and `telemetry_clean_marts`, and the dashboard queries the last two by name.

### 4. Start the dashboard

Run these from the repository root, with the virtualenv from step 3 active, because the dashboard opens its connection file by relative path:

```bash
cp config/redshift_connection_template.json config/redshift_connection.json
# edit config/redshift_connection.json: host, username, password
streamlit run python/streamlit_dashboard.py
```

`config/redshift_connection.json` is git-ignored. Streamlit serves the dashboard at `http://localhost:8501` by default.

## Tests

```bash
python3 -m pip install -r requirements-dev.txt
python3 -m pytest
```

Any virtualenv with Python 3.10 or later will do; the suite was run on Python 3.10, 3.13 and 3.14. The 12 tests in `tests/test_pipeline.py` run the generator and the cleaner as command line programs inside temporary directories and check that:

- the generator writes only JSON Lines to stdout, with known engine ids, readings 1 to 5 seconds apart and exact duplicates appended after them, and that with a seeded random module the three fault rates are close to their configured values;
- the cleaner's output has no repeated (`timestamp`, `engine_id`) pair, keeps every value inside the ranges it enforces, matches an independent restatement of the cleaning rules row for row, and comes with logged counts that add up;
- each cleaning rule holds for a set of handwritten records, the default output path is used when none is given, and a missing input file gives exit status 1;
- `data/telemetry_clean.csv` is what the cleaner produces from `data/telemetry_raw.csv`, the engine names in the dbt mart match the generator, and the cleaner's columns are declared in the dbt source.

The 9 tests in `tests/test_dashboard.py` cover the dashboard without a warehouse:

- 5 static checks compile `python/streamlit_dashboard.py` and read its syntax tree without importing it. They fail on a syntax error, on a missing function or a missing `main()` call, on a change to the four section headings, on a query whose schema, table or columns the dbt models do not define, and on page text that holds an emoji, the word "real-time" or a percentage typed into a string.
- 4 render checks run the whole page with Streamlit's `AppTest`, with `psycopg2.connect` and `pandas.read_sql` replaced so that the queries return stand-in tables: with data, without anomalies, with empty tables and without a connection file. They need the packages in `requirements-stable.txt` and are skipped when those are not installed. To run them, install `requirements-dev.txt` into the virtualenv from [step 3](#3-run-dbt).

`.github/workflows/ci.yml` runs on every push and pull request with Python 3.12. It runs the pytest suite, installs dbt-core 1.8.7 and dbt-redshift 1.8.1, runs `dbt deps`, and runs `dbt parse` with a copy of `dbt/profiles.yml.example` and placeholder values. `dbt parse` fails on broken Jinja, on a `ref()` or `source()` that does not resolve and on missing packages. It does not check the SQL and does not connect to a warehouse, so the models are never executed and the 44 dbt tests never run in CI. The 4 render checks for the dashboard are skipped there as well, because the workflow does not install the dashboard's dependencies.

## Sample data

`data/` holds one run from 2025-07-11: 1,047 generated records (1,000 unique readings plus 47 injected duplicates) in `telemetry_raw.csv`, and 999 cleaned rows in `telemetry_clean.csv`, 30 of which have at least one missing measurement. The cleaner dropped one reading for a temperature below -273.15 and corrected 12 (5 negative pressures and 7 zero fuel flows). Running the cleaner on `telemetry_raw.csv` reproduces `telemetry_clean.csv` exactly.

The engine ids in both files were changed to the `ENG-` prefix when the project was renamed. Timestamps and measurements are as generated.

## Limitations

- The Airbyte connection and the rows of the two dimension tables live outside the repository. The warehouse steps cannot be reproduced from a fresh clone without recreating them: the repository has the dimension DDL but no statements that populate the tables.
- `fact_telemetry_readings` refers to the dimension tables by hard-coded name rather than through `source()` or `ref()`, so they do not appear in dbt lineage.
- `sql/star_schema_design.sql` also defines a fact table and a bridge table in `telemetry_clean`. The dbt project builds its own fact table in `telemetry_clean_core` and uses only the two dimension tables from that file.
- `engine_performance_summary` does not read `dim_engines`. It derives `engine_name` from the five engine ids with a `CASE` expression and fills `engine_type`, `manufacturer` ("Example Aerospace", a made-up name), `operational_status` and `installation_date` with constants.
- `models/schema.yml` ends with a top-level `tests:` block that holds a grain check as inline SQL. dbt does not turn that block into a test (checked with dbt-core 1.8.7 and 1.12.5), so the check never runs.
- The `ANALYZE` post-hook on `fact_telemetry_readings` is configured both in `dbt_project.yml` and in the model, so it runs twice.
- A line that holds a bare JSON number, `true`, `false` or `null`, or a record whose timestamp is not a string, stops the cleaner at that line: it writes the rows read so far, logs the error and still exits with status 0. The generator never emits such lines, and the tests do not cover them.
- The cleaner's lower bound for temperature is -273.15, absolute zero in Celsius, while the generator and the dbt models label temperature as Fahrenheit.
- In the dashboard, the "Auto Refresh" checkbox is not connected to any refresh logic, and the status banner, the sidebar objectives and the footer status line (including its data quality and response time figures) are static text. Its subtitle and one section heading say "Real-time", but each page load only re-reads the tables that the last `dbt run` built.

## Author

Shubhrose Singh. First written in June and July 2025. This repository is a cleaned copy of that project, and its history starts at the import commit.

## License

MIT. See `LICENSE`.

The skeleton of the dbt project was generated by `dbt init` from the starter project that ships with dbt-core (dbt Labs, Apache-2.0). `dbt_utils` is downloaded by `dbt deps` and is not part of this repository.
