/*
    Engine Performance Summary Mart - Simplified
    
    One row per engine, with:
    - Averages, minimums, maximums and standard deviations of the measurements
    - Anomaly counts and the anomaly rate
    - Ranks by performance score, anomaly rate and fuel efficiency ratio
    - A health status derived from the anomaly rate
    
    Note: Simplified to avoid dimension table join issues. dim_engines is not
    read: engine_name comes from a CASE on engine_id, and engine_type,
    manufacturer, operational_status and installation_date are constants.
*/

{{ config(
    materialized='table',
    schema='marts'
) }}

WITH fact_readings AS (
    SELECT * FROM {{ ref('fact_telemetry_readings') }}
),

engine_metrics AS (
    SELECT 
        f.engine_id,
        
        -- Create engine name from engine_id  
        CASE 
            WHEN f.engine_id = 'ENG-001' THEN 'Engine Alpha'
            WHEN f.engine_id = 'ENG-002' THEN 'Engine Beta'  
            WHEN f.engine_id = 'ENG-003' THEN 'Engine Gamma'
            WHEN f.engine_id = 'ENG-004' THEN 'Engine Delta'
            WHEN f.engine_id = 'ENG-005' THEN 'Engine Epsilon'
            ELSE f.engine_id || ' (Unknown)'
        END AS engine_name,
        
        'Rocket Engine' AS engine_type,
        'Example Aerospace' AS manufacturer,
        'ACTIVE' AS operational_status,
        '2024-01-01'::date AS installation_date,
        
        -- Performance metrics
        COUNT(*) AS total_readings,
        AVG(f.chamber_pressure_psi) AS avg_chamber_pressure_psi,
        AVG(f.fuel_flow_kg_per_sec) AS avg_fuel_flow_kg_per_sec,
        AVG(f.temperature_fahrenheit) AS avg_temperature_fahrenheit,
        AVG(f.fuel_efficiency_ratio) AS avg_fuel_efficiency_ratio,
        AVG(f.performance_score) AS avg_performance_score,
        
        -- Statistical measures
        STDDEV(f.chamber_pressure_psi) AS stddev_chamber_pressure,
        STDDEV(f.fuel_flow_kg_per_sec) AS stddev_fuel_flow,
        STDDEV(f.temperature_fahrenheit) AS stddev_temperature,
        
        -- Min/Max values
        MIN(f.chamber_pressure_psi) AS min_chamber_pressure_psi,
        MAX(f.chamber_pressure_psi) AS max_chamber_pressure_psi,
        MIN(f.fuel_flow_kg_per_sec) AS min_fuel_flow_kg_per_sec,
        MAX(f.fuel_flow_kg_per_sec) AS max_fuel_flow_kg_per_sec,
        MIN(f.temperature_fahrenheit) AS min_temperature_fahrenheit,
        MAX(f.temperature_fahrenheit) AS max_temperature_fahrenheit,
        
        -- Anomaly analysis
        COUNT(CASE WHEN f.is_anomaly THEN 1 END) AS anomaly_count,
        ROUND(
            COUNT(CASE WHEN f.is_anomaly THEN 1 END) * 100.0 / COUNT(*), 
            2
        ) AS anomaly_rate_percent,
        
        -- Specific anomaly types
        COUNT(CASE WHEN f.anomaly_type LIKE '%PRESSURE%' THEN 1 END) AS pressure_anomalies,
        COUNT(CASE WHEN f.anomaly_type LIKE '%FUEL%' THEN 1 END) AS fuel_anomalies,
        COUNT(CASE WHEN f.anomaly_type LIKE '%TEMPERATURE%' THEN 1 END) AS temperature_anomalies,
        
        -- Average as a percentage of a fixed limit (350 psi, 200 kg/s, 4000 °F)
        ROUND(AVG(f.chamber_pressure_psi) * 100.0 / 350.0, 2) AS pressure_utilization_percent,
        ROUND(AVG(f.fuel_flow_kg_per_sec) * 100.0 / 200.0, 2) AS fuel_flow_utilization_percent,
        ROUND(AVG(f.temperature_fahrenheit) * 100.0 / 4000.0, 2) AS temperature_utilization_percent,
        
        -- Time range of data
        MIN(f.reading_timestamp::date) AS earliest_reading_date,
        MAX(f.reading_timestamp::date) AS latest_reading_date,
        
        -- No burn phase is modelled: these repeat total_readings and avg_performance_score
        COUNT(*) AS burn_phase_readings,
        AVG(f.performance_score) AS burn_phase_avg_performance,
        
        -- Data quality indicators
        COUNT(CASE WHEN f.airbyte_raw_id IS NOT NULL THEN 1 END) AS records_with_source_id,
        MIN(f.created_at) AS first_processed_at,
        MAX(f.created_at) AS last_processed_at
        
    FROM fact_readings f
    GROUP BY f.engine_id
),

ranked_engines AS (
    SELECT 
        *,
        -- Performance ranking
        RANK() OVER (ORDER BY avg_performance_score DESC) AS performance_rank,
        RANK() OVER (ORDER BY anomaly_rate_percent ASC) AS reliability_rank,
        RANK() OVER (ORDER BY avg_fuel_efficiency_ratio DESC) AS efficiency_rank,
        
        -- Health status based on anomaly rate
        CASE 
            WHEN anomaly_rate_percent <= 5 THEN 'EXCELLENT'
            WHEN anomaly_rate_percent <= 15 THEN 'GOOD'
            WHEN anomaly_rate_percent <= 30 THEN 'FAIR'
            ELSE 'NEEDS_ATTENTION'
        END AS health_status,
        
        -- Current timestamp for reporting
        GETDATE() AS report_generated_at
        
    FROM engine_metrics
)

SELECT * FROM ranked_engines
ORDER BY performance_rank 