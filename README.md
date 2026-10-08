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
| `sql/star_schema_design.sql` | Redshift DDL for the fact, dimension and bridge tables |
| `config/redshift_connection_template.json` | Template for the dashboard's connection file |
| `config/airbyte_config.json` | Placeholder Airbyte API settings; no script reads it |
| `data/` | Output of one sample run (raw and cleaned) |
| `requirements-stable.txt` | Pinned dbt, Streamlit, pandas, Plotly and psycopg2 versions |

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
cd dbt/telemetry_analytics
dbt deps
dbt run
dbt test
```

The pinned versions were released between December 2022 and September 2024. The pinned pandas, grpcio and psycopg2-binary releases have no wheels for Python 3.12 or later, so use Python 3.11 or earlier. The install was checked on Python 3.10. `packages.yml` asks for `dbt_utils` 1.3.0, which two of the tests use.

dbt needs a profile named `telemetry_analytics` (the `profile` set in `dbt_project.yml`). Put it in `~/.dbt/profiles.yml`, or keep it elsewhere and pass `--profiles-dir`. Read the password from the environment rather than writing it into the file:

```yaml
telemetry_analytics:
  target: dev
  outputs:
    dev:
      type: redshift
      host: <workgroup>.<account-id>.<region>.redshift-serverless.amazonaws.com
      port: 5439
      user: <user>
      password: "{{ env_var('REDSHIFT_PASSWORD') }}"
      dbname: dev
      schema: telemetry_clean
      threads: 4
      sslmode: require
```

Keep `schema: telemetry_clean`. With dbt's default schema naming the models are then built in `telemetry_clean_staging`, `telemetry_clean_core` and `telemetry_clean_marts`, which are the names the dashboard queries.

### 4. Start the dashboard

Run these from the repository root, with the virtualenv from step 3 active, because the dashboard opens its connection file by relative path:

```bash
cp config/redshift_connection_template.json config/redshift_connection.json
# edit config/redshift_connection.json: host, username, password
streamlit run python/streamlit_dashboard.py
```

`config/redshift_connection.json` is git-ignored. Streamlit serves the dashboard at `http://localhost:8501` by default.

## Sample data

`data/` holds one run from 2025-07-11: 1,047 generated records (1,000 unique readings plus 47 injected duplicates) in `telemetry_raw.csv`, and 999 cleaned rows in `telemetry_clean.csv`, 30 of which have at least one missing measurement. The cleaner dropped one reading for a temperature below -273.15 and corrected 12 (5 negative pressures and 7 zero fuel flows). Running the cleaner on `telemetry_raw.csv` reproduces `telemetry_clean.csv` exactly.

The engine ids in both files were changed to the `ENG-` prefix when the project was renamed. Timestamps and measurements are as generated.

## Limitations

- The Airbyte connection and the rows of the two dimension tables live outside the repository. The warehouse steps cannot be reproduced from a fresh clone without recreating them: the repository has the dimension DDL but no statements that populate the tables.
- `dbt deps` fails with the pinned dbt-core 1.8.7: the committed `package-lock.yml` has a `name` key that this version rejects as malformed.
- `fact_telemetry_readings` refers to the dimension tables by hard-coded name rather than through `source()` or `ref()`, so they do not appear in dbt lineage.
- `sql/star_schema_design.sql` also defines a fact table and a bridge table in `telemetry_clean`. The dbt project builds its own fact table in `telemetry_clean_core` and uses only the two dimension tables from that file.
- `engine_performance_summary` does not read `dim_engines`. It derives `engine_name` from the five engine ids with a `CASE` expression and fills `engine_type`, `manufacturer` ("Example Aerospace", a made-up name), `operational_status` and `installation_date` with constants.
- `models/schema.yml` ends with a top-level `tests:` block that holds a grain check as inline SQL. dbt-core 1.8.7 does not turn that block into a test, so the check never runs.
- The `ANALYZE` post-hook on `fact_telemetry_readings` is configured both in `dbt_project.yml` and in the model, so it runs twice.
- The cleaner's lower bound for temperature is -273.15, absolute zero in Celsius, while the generator and the dbt models label temperature as Fahrenheit.
- In the dashboard, the "Auto Refresh" checkbox is not connected to any refresh logic, and the status banner, the sidebar objectives and the footer status line (including its data quality and response time figures) are static text.

## Author

Shubhrose Singh. First written in June and July 2025. This repository is a cleaned copy of that project, and its history starts at the import commit.
