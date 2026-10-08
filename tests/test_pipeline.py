"""Tests for the telemetry generator and the cleaner.

The two scripts are run the way the README runs them, as command line
programs, always inside a pytest temporary directory: the cleaner writes
errors.log, and by default data/telemetry_clean.csv, relative to the working
directory.

The generator has no seed option, so the command line tests assert only
properties that hold for every possible output. The fault rates are checked
in-process with a seeded random module.
"""

import csv
import json
import os
import random
import re
import subprocess
import sys
from datetime import datetime
from pathlib import Path

import pytest

# Importable because pytest.ini puts python/ on sys.path.
import generate_telemetry

REPO = Path(__file__).resolve().parent.parent
GENERATOR = REPO / "python" / "generate_telemetry.py"
CLEANER = REPO / "python" / "ingest_and_clean.py"

ENGINE_IDS = {"ENG-001", "ENG-002", "ENG-003", "ENG-004", "ENG-005"}
MEASUREMENTS = ("chamber_pressure", "fuel_flow", "temperature")
CSV_HEADER = ["timestamp", "engine_id", *MEASUREMENTS]

# The cleaner drops readings whose temperature is below this value.
MIN_TEMPERATURE = -273.15

SUMMARY_LABELS = {
    "total": "Total records processed",
    "valid": "Valid records",
    "dropped": "Dropped records",
    "corrected": "Corrected records",
    "duplicates": "Duplicate records removed",
    "parsing_errors": "Parsing errors",
}


# ---------------------------------------------------------------- helpers


def run_script(script, cwd, *args, stdin_text=""):
    """Run one of the pipeline scripts with cwd as its working directory."""
    return subprocess.run(
        [sys.executable, str(script), *args],
        cwd=cwd,
        input=stdin_text,
        capture_output=True,
        encoding="utf-8",
        # The cleaner logs a non-ASCII arrow; keep its output UTF-8 on every platform.
        env={**os.environ, "PYTHONUTF8": "1"},
    )


def read_jsonl(text):
    return [json.loads(line) for line in text.splitlines() if line.strip()]


def read_csv(path):
    """Return (header, rows) with each row as a dict of strings."""
    with open(path, newline="", encoding="utf-8") as handle:
        lines = list(csv.reader(handle))
    header = lines[0]
    return header, [dict(zip(header, line)) for line in lines[1:]]


def read_summary(log_text):
    """Extract the run summary the cleaner logs at the end."""
    summary = {}
    for key, label in SUMMARY_LABELS.items():
        match = re.search(rf"{re.escape(label)}: (\d+)", log_text)
        assert match, f"summary line missing: {label}"
        summary[key] = int(match.group(1))
    return summary


def below_minimum_temperature(record):
    return "temperature" in record and record["temperature"] < MIN_TEMPERATURE


def needs_correction(record):
    return record.get("chamber_pressure", 0) < 0 or record.get("fuel_flow") == 0


def expected_clean_rows(records):
    """Restate the cleaning rules for generator output, independently of the cleaner.

    Generator records always carry a valid timestamp, an engine id and numeric
    measurements, so the only reason to drop one is its temperature.
    """
    seen = set()
    rows = []
    for record in records:
        if below_minimum_temperature(record):
            continue
        key = (record["timestamp"], record["engine_id"])
        if key in seen:
            continue
        seen.add(key)
        row = dict(record)
        if row.get("chamber_pressure", 0) < 0:
            row["chamber_pressure"] = abs(row["chamber_pressure"])
        if row.get("fuel_flow") == 0:
            row["fuel_flow"] = 0.1
        rows.append(row)
    return rows


def assert_generator_invariants(records, requested):
    """Properties every batch has: `requested` readings, then exact duplicates."""
    readings, extras = records[:requested], records[requested:]
    assert len(readings) == requested

    for record in records:
        assert set(record) <= set(CSV_HEADER)
        assert record["engine_id"] in ENGINE_IDS
        present = [field for field in MEASUREMENTS if field in record]
        assert present, "a reading never loses all three measurements"
        for field in present:
            assert isinstance(record[field], (int, float))
            assert not isinstance(record[field], bool)

    # Readings come in time order, 1 to 5 seconds apart.
    stamps = [datetime.fromisoformat(record["timestamp"]) for record in readings]
    for earlier, later in zip(stamps, stamps[1:]):
        assert 1.0 <= (later - earlier).total_seconds() <= 5.0

    keys = {(record["timestamp"], record["engine_id"]) for record in readings}
    assert len(keys) == requested

    # Injected duplicates are appended after the readings and copy one exactly.
    for extra in extras:
        assert extra in readings


def far_out_of_range(record):
    """True for the values the generator injects as sensor errors or failures.

    Normal readings sit near 260-290 psi, 125-145 kg/s and 4050-4150 degrees,
    many standard deviations inside these windows.
    """
    pressure = record.get("chamber_pressure")
    fuel_flow = record.get("fuel_flow")
    temperature = record.get("temperature")
    return (
        (pressure is not None and not 100 <= pressure < 400)
        or (fuel_flow is not None and not 0 < fuel_flow < 300)
        or (temperature is not None and not 0 < temperature < 5000)
    )


@pytest.fixture
def seeded_random(request):
    """Seed the global random module for one test and restore it afterwards."""
    state = random.getstate()
    random.seed(request.param)
    yield request.param
    random.setstate(state)


# -------------------------------------------------------------- generator


def test_generator_writes_only_json_lines_to_stdout(tmp_path):
    result = run_script(GENERATOR, tmp_path, "300")

    assert result.returncode == 0
    lines = result.stdout.splitlines()
    assert all(line.startswith("{") for line in lines)
    assert_generator_invariants([json.loads(line) for line in lines], 300)

    progress = result.stderr.splitlines()
    assert progress and all(line.startswith("#") for line in progress)
    assert list(tmp_path.iterdir()) == [], "the generator writes no files"


def test_generator_defaults_to_100_readings(tmp_path):
    result = run_script(GENERATOR, tmp_path)

    assert result.returncode == 0
    assert_generator_invariants(read_jsonl(result.stdout), 100)


def test_generator_rejects_a_count_that_is_not_an_integer(tmp_path):
    result = run_script(GENERATOR, tmp_path, "ten")

    assert result.returncode == 1
    assert result.stdout == ""
    assert "must be an integer" in result.stderr


@pytest.mark.parametrize("seeded_random", [1, 20250711], indirect=True)
def test_generator_injects_each_kind_of_fault(seeded_random):
    requested = 5000
    records = generate_telemetry.TelemetryGenerator().generate_telemetry_batch(requested)
    assert_generator_invariants(records, requested)

    readings = records[:requested]
    missing = sum(1 for r in readings if any(field not in r for field in MEASUREMENTS))
    duplicates = len(records) - requested
    out_of_range = sum(1 for r in readings if far_out_of_range(r))

    # Configured rates are 3%, 4% and 5% (plus a few critical failures). The
    # bounds are about six standard deviations wide for this sample size.
    assert 0.015 <= missing / requested <= 0.045
    assert 0.025 <= duplicates / requested <= 0.055
    assert 0.030 <= out_of_range / requested <= 0.070


# ---------------------------------------------------------------- cleaner


def test_cleaner_output_follows_the_cleaning_rules(tmp_path):
    raw_text = run_script(GENERATOR, tmp_path, "2000").stdout
    (tmp_path / "raw.jsonl").write_text(raw_text, encoding="utf-8")

    result = run_script(CLEANER, tmp_path, "--input", "raw.jsonl", "--output", "out/clean.csv")

    assert result.returncode == 0
    records = read_jsonl(raw_text)
    header, rows = read_csv(tmp_path / "out" / "clean.csv")
    assert header == CSV_HEADER

    # No (timestamp, engine_id) pair appears twice.
    keys = [(row["timestamp"], row["engine_id"]) for row in rows]
    assert len(keys) == len(set(keys))

    # Values are within the ranges the cleaner enforces. Missing measurements stay empty.
    for row in rows:
        datetime.fromisoformat(row["timestamp"])
        assert row["engine_id"] in ENGINE_IDS
        if row["chamber_pressure"]:
            assert float(row["chamber_pressure"]) >= 0
        if row["fuel_flow"]:
            assert float(row["fuel_flow"]) != 0
        if row["temperature"]:
            assert float(row["temperature"]) >= MIN_TEMPERATURE

    # The file holds exactly the rows the rules call for, in input order.
    expected = expected_clean_rows(records)
    assert len(rows) == len(expected)
    for row, want in zip(rows, expected):
        assert (row["timestamp"], row["engine_id"]) == (want["timestamp"], want["engine_id"])
        for field in MEASUREMENTS:
            if field in want:
                assert float(row[field]) == want[field]
            else:
                assert row[field] == ""

    # The logged counts add up and agree with the input and the output.
    summary = read_summary(result.stderr)
    assert summary["total"] == len(records)
    assert summary["parsing_errors"] == 0
    assert summary["valid"] + summary["dropped"] == summary["total"]
    assert summary["dropped"] == sum(1 for r in records if below_minimum_temperature(r))
    assert summary["corrected"] == sum(
        1 for r in records if needs_correction(r) and not below_minimum_temperature(r)
    )
    assert summary["valid"] - summary["duplicates"] == len(rows)

    # The same summary is written to errors.log in the working directory.
    assert read_summary((tmp_path / "errors.log").read_text(encoding="utf-8")) == summary


def test_cleaner_applies_each_rule_to_handwritten_records(tmp_path):
    base = {"chamber_pressure": 280.0, "fuel_flow": 120.0, "temperature": 4000.0}
    first = {"timestamp": "2025-07-11T16:00:00", "engine_id": "ENG-001", **base}
    lines = [
        # Kept as written.
        json.dumps(first),
        # Dropped as duplicates of the first record: an exact copy, then the same key with other values.
        json.dumps(first),
        json.dumps({**first, "chamber_pressure": 1.0}),
        # Kept: same timestamp, different engine.
        json.dumps({**first, "engine_id": "ENG-002"}),
        # Corrected: negative pressure, zero fuel flow, and both in one record.
        json.dumps({"timestamp": "2025-07-11T16:00:01", "engine_id": "ENG-001", **base, "chamber_pressure": -79.5}),
        json.dumps({"timestamp": "2025-07-11T16:00:02", "engine_id": "ENG-001", **base, "fuel_flow": 0}),
        json.dumps({"timestamp": "2025-07-11T16:00:03", "engine_id": "ENG-001", **base, "chamber_pressure": -10.0, "fuel_flow": 0.0}),
        # Dropped: temperature below the minimum, with and without another fault.
        json.dumps({"timestamp": "2025-07-11T16:00:04", "engine_id": "ENG-001", **base, "temperature": -273.16}),
        json.dumps({"timestamp": "2025-07-11T16:00:05", "engine_id": "ENG-001", **base, "chamber_pressure": -5.0, "temperature": -300.0}),
        # Kept: temperature exactly at the minimum, and a very high one.
        json.dumps({"timestamp": "2025-07-11T16:00:06", "engine_id": "ENG-001", **base, "temperature": -273.15}),
        json.dumps({"timestamp": "2025-07-11T16:00:07", "engine_id": "ENG-001", **base, "temperature": 9000.0}),
        # Kept with empty cells: measurements missing.
        json.dumps({"timestamp": "2025-07-11T16:00:08", "engine_id": "ENG-001", "chamber_pressure": 270.0}),
        # Kept: a timestamp with a trailing Z and a measurement sent as a numeric string.
        json.dumps({"timestamp": "2025-07-11T16:00:09Z", "engine_id": "ENG-001", **base, "chamber_pressure": "250.5"}),
        # Dropped: required field missing, bad timestamp, non-numeric and null measurements.
        json.dumps({"timestamp": "2025-07-11T16:00:10", **base}),
        json.dumps({"engine_id": "ENG-001", **base}),
        json.dumps({"timestamp": "not-a-timestamp", "engine_id": "ENG-001", **base}),
        json.dumps({"timestamp": "2025-07-11T16:00:11", "engine_id": "ENG-001", **base, "fuel_flow": "abc"}),
        json.dumps({"timestamp": "2025-07-11T16:00:12", "engine_id": "ENG-001", **base, "temperature": None}),
        # Skipped without being counted: blank line and comment. Counted as a parsing error: broken JSON.
        "",
        "# a comment line",
        "{not json",
    ]
    (tmp_path / "raw.jsonl").write_text("\n".join(lines) + "\n", encoding="utf-8")

    result = run_script(CLEANER, tmp_path, "-i", "raw.jsonl", "-o", "clean.csv")

    assert result.returncode == 0
    header, rows = read_csv(tmp_path / "clean.csv")
    assert header == CSV_HEADER
    assert [list(row.values()) for row in rows] == [
        ["2025-07-11T16:00:00", "ENG-001", "280.0", "120.0", "4000.0"],
        ["2025-07-11T16:00:00", "ENG-002", "280.0", "120.0", "4000.0"],
        ["2025-07-11T16:00:01", "ENG-001", "79.5", "120.0", "4000.0"],
        ["2025-07-11T16:00:02", "ENG-001", "280.0", "0.1", "4000.0"],
        ["2025-07-11T16:00:03", "ENG-001", "10.0", "0.1", "4000.0"],
        ["2025-07-11T16:00:06", "ENG-001", "280.0", "120.0", "-273.15"],
        ["2025-07-11T16:00:07", "ENG-001", "280.0", "120.0", "9000.0"],
        ["2025-07-11T16:00:08", "ENG-001", "270.0", "", ""],
        ["2025-07-11T16:00:09Z", "ENG-001", "250.5", "120.0", "4000.0"],
    ]
    assert read_summary(result.stderr) == {
        "total": 18,
        "valid": 11,
        "dropped": 7,
        "corrected": 3,
        "duplicates": 2,
        "parsing_errors": 1,
    }


def test_cleaner_reads_stdin_and_writes_to_the_default_path(tmp_path):
    raw_text = run_script(GENERATOR, tmp_path, "50").stdout

    result = run_script(CLEANER, tmp_path, stdin_text=raw_text)

    assert result.returncode == 0
    header, rows = read_csv(tmp_path / "data" / "telemetry_clean.csv")
    assert header == CSV_HEADER
    assert len(rows) == len(expected_clean_rows(read_jsonl(raw_text)))


def test_cleaner_exits_with_status_1_when_the_input_file_is_missing(tmp_path):
    result = run_script(CLEANER, tmp_path, "--input", "missing.jsonl", "--output", "clean.csv")

    assert result.returncode == 1
    assert "not found" in result.stderr
    assert not (tmp_path / "clean.csv").exists()


# ------------------------------------------- sample data and dbt project


def test_sample_csv_is_the_cleaner_output_for_the_sample_raw_file(tmp_path):
    result = run_script(
        CLEANER, tmp_path, "--input", str(REPO / "data" / "telemetry_raw.csv"), "--output", "clean.csv"
    )

    assert result.returncode == 0
    header, rows = read_csv(tmp_path / "clean.csv")
    assert (header, rows) == read_csv(REPO / "data" / "telemetry_clean.csv")
    assert {row["engine_id"] for row in rows} == ENGINE_IDS

    # The figures quoted in the README.
    assert read_summary(result.stderr) == {
        "total": 1047,
        "valid": 1046,
        "dropped": 1,
        "corrected": 12,
        "duplicates": 47,
        "parsing_errors": 0,
    }
    assert len(rows) == 999
    assert sum(1 for row in rows if "" in row.values()) == 30


def test_engine_names_in_the_dbt_mart_match_the_generator():
    sql = (
        REPO / "dbt" / "telemetry_analytics" / "models" / "marts" / "engine_performance_summary.sql"
    ).read_text(encoding="utf-8")
    in_mart = dict(re.findall(r"WHEN f\.engine_id = '([^']+)' THEN '([^']+)'", sql))
    engines = generate_telemetry.TelemetryGenerator().engines

    assert set(engines) == ENGINE_IDS
    assert in_mart == {engine_id: config["name"] for engine_id, config in engines.items()}


def test_cleaner_columns_are_declared_in_the_dbt_source():
    sources = (
        REPO / "dbt" / "telemetry_analytics" / "models" / "staging" / "sources.yml"
    ).read_text(encoding="utf-8")
    declared = set(re.findall(r"- name: (\w+)", sources))

    assert set(CSV_HEADER) <= declared
