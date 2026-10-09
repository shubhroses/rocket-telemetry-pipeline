"""Tests for the Streamlit dashboard.

Importing python/streamlit_dashboard.py needs Streamlit, pandas, Plotly and
psycopg2, and showing anything needs a Redshift connection. The tests therefore
come in two groups:

- Static checks, which run everywhere. They compile the file and read its
  syntax tree without importing it, so a syntax error, a missing page section
  or a query that no longer matches the dbt models fails the suite.
- Render checks, which run the whole page with Streamlit's AppTest against
  stand-in tables. They are skipped unless the dashboard's dependencies
  (requirements-stable.txt) are installed, which they are not in CI.
"""

import ast
import json
import re
from pathlib import Path
from unittest import mock

import pytest

REPO = Path(__file__).resolve().parent.parent
DASHBOARD = REPO / "python" / "streamlit_dashboard.py"
MODELS = REPO / "dbt" / "telemetry_analytics" / "models"

FUNCTIONS = {
    "init_database_connection",
    "load_engine_performance",
    "load_daily_trends",
    "load_latest_readings",
    "create_performance_gauge",
    "create_time_series_chart",
    "main",
}

# The st.subheader() calls in main(), top to bottom.
SECTIONS = [
    "Engine Performance Overview",
    "Engine Summary",
    "Latest Readings",
    "Anomalies in the Latest Readings",
]

# Loader function -> the dbt model its query reads.
LOADERS = {
    "load_engine_performance": "engine_performance_summary",
    "load_daily_trends": "daily_anomaly_trends",
    "load_latest_readings": "fact_telemetry_readings",
}


# ---------------------------------------------------------- static checks


@pytest.fixture(scope="module")
def tree():
    return ast.parse(DASHBOARD.read_text(encoding="utf-8"), filename=str(DASHBOARD))


def top_level_functions(tree):
    return {node.name: node for node in tree.body if isinstance(node, ast.FunctionDef)}


def string_literals(node):
    """Every piece of literal text under node, the fixed parts of f-strings included."""
    return [child.value for child in ast.walk(node) if isinstance(child, ast.Constant) and isinstance(child.value, str)]


def query_text(function):
    """The SQL a loader assigns to `query`, on one line, with f-string fields shown as `?`."""
    for node in ast.walk(function):
        if isinstance(node, ast.Assign) and ast.unparse(node.targets[0]) == "query":
            value = node.value
            if isinstance(value, ast.JoinedStr):
                parts = [part.value if isinstance(part, ast.Constant) else "?" for part in value.values]
            else:
                parts = [value.value]
            return " ".join("".join(parts).split())
    raise AssertionError(f"{function.name} assigns no query")


def without_sql_comments(sql):
    return re.sub(r"--[^\n]*", "", re.sub(r"/\*.*?\*/", "", sql, flags=re.S))


def test_dashboard_compiles():
    compile(DASHBOARD.read_text(encoding="utf-8"), str(DASHBOARD), "exec")


def test_dashboard_defines_its_functions_and_runs_main_as_a_script(tree):
    assert FUNCTIONS <= set(top_level_functions(tree))

    # "streamlit run" executes the file as __main__. Without this block the page stays empty.
    script_guard = tree.body[-1]
    assert isinstance(script_guard, ast.If)
    assert ast.unparse(script_guard.test) == "__name__ == '__main__'"
    assert [ast.unparse(statement) for statement in script_guard.body] == ["main()"]


def test_main_renders_the_expected_sections_in_order(tree):
    main = top_level_functions(tree)["main"]
    headings = sorted(
        (node.lineno, node.args[0].value)
        for node in ast.walk(main)
        if isinstance(node, ast.Call) and ast.unparse(node.func) == "st.subheader"
    )

    assert [text for _, text in headings] == SECTIONS


def test_queries_name_tables_and_columns_that_the_dbt_models_define(tree):
    # dbt builds each model in <profile schema>_<the model's custom schema>.
    profile = (REPO / "dbt" / "profiles.yml.example").read_text(encoding="utf-8")
    profile_schema = re.search(r"^\s*schema:\s*(\w+)", profile, flags=re.M).group(1)
    functions = top_level_functions(tree)

    for loader, model in LOADERS.items():
        query = query_text(functions[loader])
        match = re.match(r"SELECT (.+) FROM (\w+)\.(\w+) ORDER BY ", query)
        assert match, f"{loader}: unexpected query shape: {query}"
        columns, schema, table = match.groups()

        (model_file,) = MODELS.glob(f"*/{model}.sql")
        model_sql = without_sql_comments(model_file.read_text(encoding="utf-8"))
        custom_schema = re.search(r"schema='(\w+)'", model_sql).group(1)
        assert (schema, table) == (f"{profile_schema}_{custom_schema}", model)

        for column in columns.split(", "):
            assert re.search(rf"\b{column}\b", model_sql), f"{model} defines no column {column}"


def test_text_is_plain_and_no_figure_is_typed_into_the_page(tree):
    for text in string_literals(tree):
        assert not re.search(r"real[- ]?time", text, flags=re.IGNORECASE), text
        # No emoji or other symbols: ASCII and the degree sign of the temperature unit only.
        assert all(ord(character) < 128 for character in text.replace("°", "")), text

    # Every percentage on the page is formatted from the loaded tables. None is written out.
    for text in string_literals(top_level_functions(tree)["main"]):
        assert not re.search(r"\d\s*%", text), text


# ---------------------------------------------------------- render checks


def stand_in_tables(*, with_anomalies=True):
    """Small tables with the columns and types that the three queries return."""
    import pandas as pd

    engines = pd.DataFrame(
        {
            "engine_id": ["ENG-003", "ENG-001", "ENG-005", "ENG-004", "ENG-002"],
            "engine_name": ["Engine Gamma", "Engine Alpha", "Engine Epsilon", "Engine Delta", "Engine Beta"],
            "avg_performance_score": [97.25, 95.5, 93.0, 88.125, 61.0],
            "anomaly_rate_percent": [1.02, 3.5, 6.25, 17.0, 31.5],
            "health_status": ["EXCELLENT", "EXCELLENT", "GOOD", "FAIR", "NEEDS_ATTENTION"],
            "total_readings": [196, 200, 208, 200, 195],
            "last_processed_at": pd.to_datetime(["2025-07-11 23:10:05"] * 5),
        }
    )
    days = pd.DataFrame(
        {
            "date_actual": pd.to_datetime(["2025-07-11", "2025-07-10"]).date,
            "total_readings": [600, 399],
            "total_anomalies": [72, 20],
            "daily_anomaly_rate_percent": [12.0, 5.01],
            "avg_daily_performance_score": [91.26, 95.0],
            "alert_flag": ["CRITICAL_HEALTH_ALERT" if with_anomalies else None, None],
        }
    )
    anomaly_types = [None, None, None, "HIGH_PRESSURE", "LOW_FUEL_FLOW", None] if with_anomalies else [None] * 6
    readings = pd.DataFrame(
        {
            "reading_timestamp": pd.to_datetime([f"2025-07-11 16:20:{second}" for second in (31, 28, 24, 21, 17, 15)]),
            "engine_id": ["ENG-001", "ENG-002", "ENG-003", "ENG-001", "ENG-002", "ENG-004"],
            "chamber_pressure_psi": [282.1, 262.5, 288.0, 512.75, 259.9, 273.0],
            "fuel_flow_kg_per_sec": [138.0, 125.5, 142.25, 137.0, 0.1, 132.0],
            "temperature_fahrenheit": [4071.0, 4150.5, 4048.0, 4069.0, 4149.0, 4108.0],
            "performance_score": [100.0, 100.0, 100.0, 60.0, 70.0, 100.0],
            "is_anomaly": [kind is not None for kind in anomaly_types],
            "anomaly_type": anomaly_types,
            "health_status": ["EXCELLENT", "EXCELLENT", "EXCELLENT", "GOOD", "GOOD", "EXCELLENT"],
        }
    )
    return {
        "engine_performance_summary": engines,
        "daily_anomaly_trends": days,
        "fact_telemetry_readings": readings,
    }


@pytest.fixture
def render(tmp_path, monkeypatch):
    """Return a function that runs the page on the given tables instead of a warehouse."""
    for module in ("pandas", "plotly", "psycopg2"):
        pytest.importorskip(module)
    streamlit = pytest.importorskip("streamlit")
    testing = pytest.importorskip("streamlit.testing.v1")

    # Streamlit's caches live as long as the process. Start each test with empty ones.
    streamlit.cache_data.clear()
    streamlit.cache_resource.clear()

    def run(tables, *, connection_file=True):
        if connection_file:
            settings = {"host": "stand-in", "port": 5439, "database": "dev", "username": "u", "password": "p"}
            (tmp_path / "config").mkdir(exist_ok=True)
            (tmp_path / "config" / "redshift_connection.json").write_text(json.dumps(settings), encoding="utf-8")
        # The dashboard opens config/redshift_connection.json relative to the working directory.
        monkeypatch.chdir(tmp_path)

        def read_sql(query, connection):
            (table,) = [name for name in tables if name in query]
            return tables[table].copy()

        with mock.patch("psycopg2.connect") as connect, mock.patch("pandas.read_sql", read_sql):
            page = testing.AppTest.from_file(str(DASHBOARD), default_timeout=30).run()
        assert not page.exception
        return page, connect

    return run


def metrics(page):
    return [(metric.label, metric.value, metric.delta) for metric in page.metric]


def test_page_shows_every_section_when_the_tables_have_data(render):
    page, connect = render(stand_in_tables())

    assert [heading.value for heading in page.subheader] == SECTIONS
    assert metrics(page) == [
        ("Engines", "5", "3 GOOD or better"),
        ("Anomaly Rate (latest day)", "12.0%", ""),
        ("Avg Score (latest day)", "91.3/100", ""),
        ("Latest Reading (2025-07-11)", "16:20:31", ""),
    ]
    assert not page.error
    assert any("Alert for the latest day:</strong> CRITICAL_HEALTH_ALERT" in text.value for text in page.markdown)

    # One query per table, each on its own connection, closed again.
    assert connect.call_count == 3
    assert connect.return_value.close.call_count == 3

    summary, anomalies = (table.value for table in page.dataframe)
    assert list(summary["engine_id"]) == ["ENG-003", "ENG-001", "ENG-005", "ENG-004", "ENG-002"]
    assert list(anomalies["anomaly_type"]) == ["HIGH_PRESSURE", "LOW_FUEL_FLOW"]


def test_page_says_so_when_the_latest_readings_have_no_anomaly(render):
    page, _ = render(stand_in_tables(with_anomalies=False))

    assert [heading.value for heading in page.subheader] == SECTIONS
    assert [message.value for message in page.success] == ["No anomalies in the latest readings."]
    assert not any("Alert for the latest day" in text.value for text in page.markdown)
    assert len(page.dataframe) == 1


def test_page_stops_with_an_error_when_the_engine_summary_is_empty(render):
    page, _ = render({name: table.iloc[0:0] for name, table in stand_in_tables().items()})

    assert [message.value for message in page.error] == ["No telemetry data available. Check the database connection."]
    assert not page.subheader
    assert not page.metric


def test_page_reports_a_missing_connection_file_without_connecting(render):
    page, connect = render(stand_in_tables(), connection_file=False)

    errors = [message.value for message in page.error]
    assert errors[0].startswith("Could not load connection settings from config/redshift_connection.json")
    assert errors[-1] == "No telemetry data available. Check the database connection."
    assert connect.call_count == 0
    assert not page.subheader


def test_page_recovers_once_the_connection_file_exists(render):
    page, _ = render(stand_in_tables(), connection_file=False)
    assert page.error

    # The page is opened again in the same server process, now with the file in place.
    page, connect = render(stand_in_tables())

    assert not page.error
    assert [heading.value for heading in page.subheader] == SECTIONS
    assert connect.call_count == 3
