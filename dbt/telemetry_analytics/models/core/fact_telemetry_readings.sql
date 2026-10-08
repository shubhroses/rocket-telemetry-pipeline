/*
    Fact Table: Telemetry Readings (Star Schema Core)
    
    Transforms staging telemetry data into a dimensional fact table for analytics
    and reporting with proper foreign key relationships.
    
    SOURCE: stg_telemetry_readings (staging layer)
    
    STAR SCHEMA DESIGN:
    - Foreign Keys: engine_key (→ dim_engines), metric foreign keys (→ dim_telemetry_metrics)
    - Calculated Keys: time_key (YYYYMMDDHH format for time-based analysis)
    - Measures: pressure, fuel_flow, temperature, efficiency_ratio, performance_score
    - Degenerate Dimensions: airbyte_raw_id, engine_id, reading_timestamp
    
    KEY TRANSFORMATIONS:
    - Dimension Lookups: Maps engine_id to engine_key via dim_engines
    - Metric Lookups: Maps each telemetry metric to dim_telemetry_metrics
    - Time Key Generation: Creates hourly time keys (YYYYMMDDHH format)
    - Health Classification: Maps performance_score to categorical status:
        * 80+ = EXCELLENT | 60-79 = GOOD | 40-59 = FAIR | 20-39 = POOR | <20 = CRITICAL
    - Surrogate Keys: Generates unique reading_key for each record
    
    MATERIALIZATION: Table with ANALYZE post-hook for query performance
    
    USAGE: Powers telemetry dashboards, engine performance analytics, and anomaly detection
*/

{{ config(
    materialized='table',
    schema='core',
    post_hook=['ANALYZE {{ this }}']
) }}

WITH staging_readings AS (
    SELECT * FROM {{ ref('stg_telemetry_readings') }}
),

engine_dim AS (
    SELECT 
        engine_key,
        engine_id
    FROM telemetry_clean.dim_engines
    WHERE is_current = TRUE
),

metrics_dim AS (
    SELECT 
        metric_key,
        metric_name,
        metric_category,
        unit_of_measure,
        normal_min_value,
        normal_max_value
    FROM telemetry_clean.dim_telemetry_metrics
),

fact_data AS (
    SELECT 
        -- Foreign keys (dimension references)
        COALESCE(e.engine_key, -1) AS engine_key,
        
        -- Metric dimension foreign keys
        COALESCE(mp.metric_key, -1) AS pressure_metric_key,
        COALESCE(mf.metric_key, -1) AS fuel_flow_metric_key,
        COALESCE(mt.metric_key, -1) AS temperature_metric_key,
        
        -- Generate simple time_key from timestamp (YYYYMMDDHH format - reduced precision)
        CAST(
            EXTRACT(YEAR FROM s.reading_timestamp) * 1000000 +
            EXTRACT(MONTH FROM s.reading_timestamp) * 10000 +
            EXTRACT(DAY FROM s.reading_timestamp) * 100 +
            EXTRACT(HOUR FROM s.reading_timestamp)
            AS BIGINT
        ) AS time_key,
        
        -- Degenerate dimensions
        s._airbyte_raw_id AS airbyte_raw_id,
        
        -- Add reading timestamp for temporal analysis
        s.reading_timestamp,
        s.engine_id,
        
        -- Facts/measures
        s.chamber_pressure_psi,
        s.fuel_flow_kg_per_sec,
        s.temperature_fahrenheit,
        s.fuel_efficiency_ratio,
        s.performance_score,
        
        -- Data quality flags
        s.is_anomaly,
        s.anomaly_type,
        
        -- Enhanced anomaly detection using metric dimension thresholds
        CASE 
            WHEN s.chamber_pressure_psi < mp.normal_min_value OR s.chamber_pressure_psi > mp.normal_max_value THEN TRUE
            WHEN s.fuel_flow_kg_per_sec < mf.normal_min_value OR s.fuel_flow_kg_per_sec > mf.normal_max_value THEN TRUE
            WHEN s.temperature_fahrenheit < mt.normal_min_value OR s.temperature_fahrenheit > mt.normal_max_value THEN TRUE
            ELSE FALSE
        END AS is_out_of_normal_range,
        
        -- Health status classification
        CASE 
            WHEN s.performance_score >= 80 THEN 'EXCELLENT'
            WHEN s.performance_score >= 60 THEN 'GOOD'
            WHEN s.performance_score >= 40 THEN 'FAIR'
            WHEN s.performance_score >= 20 THEN 'POOR'
            ELSE 'CRITICAL'
        END AS health_status,
        
        -- ETL metadata
        s.source_system,
        s.processed_at AS created_at,
        s.processed_at AS updated_at
        
    FROM staging_readings s
    LEFT JOIN engine_dim e 
        ON s.engine_id = e.engine_id
    LEFT JOIN metrics_dim mp 
        ON mp.metric_name = 'chamber_pressure'
    LEFT JOIN metrics_dim mf 
        ON mf.metric_name = 'fuel_flow'
    LEFT JOIN metrics_dim mt 
        ON mt.metric_name = 'temperature'
)

SELECT 
    -- Generate surrogate key using row_number
    ROW_NUMBER() OVER (ORDER BY time_key, engine_key) AS reading_key,
    engine_key,
    pressure_metric_key,
    fuel_flow_metric_key,
    temperature_metric_key,
    time_key,
    airbyte_raw_id,
    reading_timestamp,
    engine_id,
    chamber_pressure_psi,
    fuel_flow_kg_per_sec,
    temperature_fahrenheit,
    fuel_efficiency_ratio,
    performance_score,
    is_anomaly,
    is_out_of_normal_range,
    anomaly_type,
    health_status,
    source_system,
    created_at,
    updated_at
FROM fact_data 