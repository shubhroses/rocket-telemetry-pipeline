#!/usr/bin/env python3
"""
Rocket Engine Telemetry Dashboard

Streamlit page over the tables that dbt builds in Redshift. Every page load
runs three queries against those tables. Nothing is streamed, so the page
shows the data as of the last dbt run.

Sections:
- Headline metrics: engine count, the latest day's anomaly rate and average
  performance score, and the time of the latest reading
- Alert banner when the latest day has an alert_flag
- Performance gauge per engine and the engine summary table
- Time series of pressure, fuel flow and temperature for the latest readings
- Table of the anomalous readings among them
"""

import json
import warnings

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import psycopg2
import streamlit as st
from plotly.subplots import make_subplots

# Suppress warnings for cleaner output
warnings.filterwarnings("ignore", category=UserWarning, module="pandas")
warnings.filterwarnings("ignore", category=FutureWarning, module="_plotly_utils")
warnings.filterwarnings("ignore", category=RuntimeWarning, module="streamlit")
warnings.filterwarnings("ignore", message=".*pandas only supports SQLAlchemy.*")
warnings.filterwarnings("ignore", message=".*coroutine.*was never awaited.*")
warnings.filterwarnings("ignore", message=".*DatetimeProperties.to_pydatetime.*")

# Page configuration
st.set_page_config(page_title="Rocket Engine Telemetry Dashboard", layout="wide", initial_sidebar_state="expanded")

# Custom CSS for dashboard styling
st.markdown(
    """
<style>
    .main-header {
        font-size: 2.5rem;
        color: #FF6B35;
        text-align: center;
        margin-bottom: 2rem;
        font-weight: bold;
    }
    .dashboard-subtitle {
        font-size: 1.2rem;
        color: #2E86AB;
        text-align: center;
        margin-bottom: 2rem;
        font-style: italic;
    }
    .metric-card {
        background-color: #f0f2f6;
        padding: 1rem;
        border-radius: 0.5rem;
        border-left: 5px solid #FF6B35;
    }
    .alert-card {
        background-color: #fff2cc;
        padding: 1rem;
        border-radius: 0.5rem;
        border-left: 5px solid #ff7f0e;
    }
    .critical-alert {
        background-color: #ffebee;
        padding: 1rem;
        border-radius: 0.5rem;
        border-left: 5px solid #d32f2f;
    }
    .notice-banner {
        background: linear-gradient(90deg, #FF6B35, #F7931E);
        color: white;
        padding: 1rem;
        border-radius: 0.5rem;
        text-align: center;
        margin: 1rem 0;
    }
</style>
""",
    unsafe_allow_html=True,
)


@st.cache_resource
def init_database_connection():
    """Read the Redshift connection parameters from config/redshift_connection.json"""
    try:
        with open("config/redshift_connection.json", "r") as f:
            config = json.load(f)

        conn_params = {
            "host": config["host"],
            "port": config["port"],
            "database": config["database"],
            "user": config["username"],
            "password": config["password"],
        }

        return conn_params
    except Exception as e:
        st.error(f"Could not load connection settings from config/redshift_connection.json: {e}")
        return None


def load_engine_performance():
    """Load engine performance summary"""
    conn_params = init_database_connection()
    if not conn_params:
        return pd.DataFrame()

    query = """
        SELECT
            engine_id,
            engine_name,
            avg_performance_score,
            anomaly_rate_percent,
            health_status,
            total_readings,
            last_processed_at
        FROM telemetry_clean_marts.engine_performance_summary
        ORDER BY avg_performance_score DESC
    """

    try:
        conn = psycopg2.connect(**conn_params)
        df = pd.read_sql(query, conn)
        conn.close()
        return df
    except Exception as e:
        st.error(f"Error loading engine performance: {e}")
        return pd.DataFrame()


def load_daily_trends():
    """Load daily anomaly trends"""
    conn_params = init_database_connection()
    if not conn_params:
        return pd.DataFrame()

    query = """
        SELECT
            date_actual,
            total_readings,
            total_anomalies,
            daily_anomaly_rate_percent,
            avg_daily_performance_score,
            alert_flag
        FROM telemetry_clean_marts.daily_anomaly_trends
        ORDER BY date_actual DESC
        LIMIT 7
    """

    try:
        conn = psycopg2.connect(**conn_params)
        df = pd.read_sql(query, conn)
        conn.close()
        return df
    except Exception as e:
        st.error(f"Error loading daily trends: {e}")
        return pd.DataFrame()


def load_latest_readings(limit=50):
    """Load latest telemetry readings"""
    conn_params = init_database_connection()
    if not conn_params:
        return pd.DataFrame()

    query = f"""
        SELECT
            reading_timestamp,
            engine_id,
            chamber_pressure_psi,
            fuel_flow_kg_per_sec,
            temperature_fahrenheit,
            performance_score,
            is_anomaly,
            anomaly_type,
            health_status
        FROM telemetry_clean_core.fact_telemetry_readings
        ORDER BY reading_timestamp DESC
        LIMIT {limit}
    """

    try:
        conn = psycopg2.connect(**conn_params)
        df = pd.read_sql(query, conn)
        conn.close()
        return df
    except Exception as e:
        st.error(f"Error loading latest readings: {e}")
        return pd.DataFrame()


def create_performance_gauge(engine_data):
    """Create performance gauge charts"""
    fig = make_subplots(
        rows=1,
        cols=len(engine_data),
        subplot_titles=[f"{row['engine_id']}" for _, row in engine_data.iterrows()],
        specs=[[{"type": "indicator"}] * len(engine_data)],
    )

    for i, (_, engine) in enumerate(engine_data.iterrows()):
        score = engine["avg_performance_score"]

        # Color based on performance
        if score >= 95:
            color = "green"
        elif score >= 85:
            color = "yellow"
        else:
            color = "red"

        fig.add_trace(
            go.Indicator(
                mode="gauge+number+delta",
                value=score,
                domain={"x": [0, 1], "y": [0, 1]},
                title={"text": f"{engine['health_status']}"},
                gauge={
                    "axis": {"range": [None, 100]},
                    "bar": {"color": color},
                    "steps": [
                        {"range": [0, 70], "color": "lightgray"},
                        {"range": [70, 85], "color": "gray"},
                        {"range": [85, 100], "color": "lightgreen"},
                    ],
                    "threshold": {"line": {"color": "red", "width": 4}, "thickness": 0.75, "value": 90},
                },
            ),
            row=1,
            col=i + 1,
        )

    fig.update_layout(height=300, margin=dict(l=20, r=20, t=50, b=20))
    return fig


def create_time_series_chart(readings_data):
    """Create time series charts for telemetry parameters"""
    if readings_data.empty:
        return go.Figure()

    fig = make_subplots(
        rows=3,
        cols=1,
        subplot_titles=["Chamber Pressure (PSI)", "Fuel Flow (kg/s)", "Temperature (°F)"],
        vertical_spacing=0.1,
    )

    # Group by engine for different colors
    engines = readings_data["engine_id"].unique()
    colors = px.colors.qualitative.Set1[: len(engines)]

    for i, engine in enumerate(engines):
        engine_data = readings_data[readings_data["engine_id"] == engine].sort_values("reading_timestamp")

        # Pressure
        fig.add_trace(
            go.Scatter(
                x=engine_data["reading_timestamp"],
                y=engine_data["chamber_pressure_psi"],
                name=f"{engine} Pressure",
                line=dict(color=colors[i]),
                connectgaps=True,
            ),
            row=1,
            col=1,
        )

        # Fuel Flow
        fig.add_trace(
            go.Scatter(
                x=engine_data["reading_timestamp"],
                y=engine_data["fuel_flow_kg_per_sec"],
                name=f"{engine} Fuel",
                line=dict(color=colors[i]),
                showlegend=False,
                connectgaps=True,
            ),
            row=2,
            col=1,
        )

        # Temperature
        fig.add_trace(
            go.Scatter(
                x=engine_data["reading_timestamp"],
                y=engine_data["temperature_fahrenheit"],
                name=f"{engine} Temp",
                line=dict(color=colors[i]),
                showlegend=False,
                connectgaps=True,
            ),
            row=3,
            col=1,
        )

    fig.update_layout(height=600, margin=dict(l=20, r=20, t=50, b=20))
    return fig


def main():
    """Main dashboard application"""

    # Header
    st.markdown('<h1 class="main-header">Rocket Engine Telemetry Dashboard</h1>', unsafe_allow_html=True)

    # What the page shows
    st.markdown(
        """
    <div class="dashboard-subtitle">
        Engine performance, latest readings and anomalies as of the last dbt run
    </div>
    """,
        unsafe_allow_html=True,
    )

    # Data source notice
    st.markdown(
        """
    <div class="notice-banner">
        <strong>Synthetic telemetry demo.</strong> Every reading here was generated by python/generate_telemetry.py.
    </div>
    """,
        unsafe_allow_html=True,
    )

    # Sidebar controls
    st.sidebar.header("Controls")

    refresh_button = st.sidebar.button("Reload Data")
    if refresh_button:
        st.cache_data.clear()
        st.rerun()
    st.sidebar.caption("Runs the three warehouse queries again. The tables change only when dbt rebuilds them.")

    # Load data
    engine_performance = load_engine_performance()
    daily_trends = load_daily_trends()
    latest_readings = load_latest_readings()

    if engine_performance.empty:
        st.error("No telemetry data available. Check the database connection.")
        return

    # Key Metrics Row
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        total_engines = len(engine_performance)
        healthy_engines = len(engine_performance[engine_performance["health_status"].isin(["EXCELLENT", "GOOD"])])
        st.metric("Engines", total_engines, f"{healthy_engines} GOOD or better")

    with col2:
        if not daily_trends.empty:
            latest_anomaly_rate = daily_trends.iloc[0]["daily_anomaly_rate_percent"]
            st.metric("Anomaly Rate (latest day)", f"{latest_anomaly_rate:.1f}%")
        else:
            st.metric("Anomaly Rate (latest day)", "N/A")

    with col3:
        if not daily_trends.empty:
            avg_performance = daily_trends.iloc[0]["avg_daily_performance_score"]
            st.metric("Avg Score (latest day)", f"{avg_performance:.1f}/100")
        else:
            st.metric("Avg Score (latest day)", "N/A")

    with col4:
        if not latest_readings.empty:
            latest_time = latest_readings.iloc[0]["reading_timestamp"]
            st.metric(f"Latest Reading ({latest_time:%Y-%m-%d})", latest_time.strftime("%H:%M:%S"))
        else:
            st.metric("Latest Reading", "N/A")

    # Alert Section
    if not daily_trends.empty and daily_trends.iloc[0]["alert_flag"]:
        st.markdown(
            f"""
        <div class="critical-alert">
            <strong>Alert for the latest day:</strong> {daily_trends.iloc[0]["alert_flag"]}
        </div>
        """,
            unsafe_allow_html=True,
        )

    # Engine Performance Gauges
    st.subheader("Engine Performance Overview")
    if not engine_performance.empty:
        gauge_fig = create_performance_gauge(engine_performance)
        st.plotly_chart(gauge_fig, use_container_width=True)

    # Performance Table
    st.subheader("Engine Summary")
    if not engine_performance.empty:
        # Format the dataframe for display
        display_df = engine_performance.copy()
        display_df["avg_performance_score"] = display_df["avg_performance_score"].round(1)
        display_df["anomaly_rate_percent"] = display_df["anomaly_rate_percent"].round(1)

        st.dataframe(
            display_df,
            column_config={
                "engine_id": "Engine ID",
                "engine_name": "Engine Name",
                "avg_performance_score": st.column_config.NumberColumn("Performance Score", format="%.1f"),
                "anomaly_rate_percent": st.column_config.NumberColumn("Anomaly Rate %", format="%.1f"),
                "health_status": "Health Status",
                "total_readings": "Telemetry Readings",
            },
            use_container_width=True,
        )

    # Time Series Charts
    st.subheader("Latest Readings")
    if not latest_readings.empty:
        ts_fig = create_time_series_chart(latest_readings)
        st.plotly_chart(ts_fig, use_container_width=True)

    # Anomalies among the latest readings
    st.subheader("Anomalies in the Latest Readings")
    if not latest_readings.empty:
        # .eq(True) rather than a plain mask, so that a NULL flag counts as not anomalous.
        anomalies = latest_readings[latest_readings["is_anomaly"].eq(True)].head(10)

        if not anomalies.empty:
            st.dataframe(
                anomalies[
                    [
                        "reading_timestamp",
                        "engine_id",
                        "anomaly_type",
                        "chamber_pressure_psi",
                        "fuel_flow_kg_per_sec",
                        "temperature_fahrenheit",
                    ]
                ],
                column_config={
                    "reading_timestamp": "Timestamp",
                    "engine_id": "Engine",
                    "anomaly_type": "Anomaly Type",
                    "chamber_pressure_psi": "Pressure (PSI)",
                    "fuel_flow_kg_per_sec": "Fuel Flow (kg/s)",
                    "temperature_fahrenheit": "Temperature (°F)",
                },
                use_container_width=True,
            )
        else:
            st.success("No anomalies in the latest readings.")


if __name__ == "__main__":
    main()
