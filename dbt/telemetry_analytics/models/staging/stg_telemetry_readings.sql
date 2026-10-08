/*
    Staging Model: Rocket Engine Telemetry Readings
    
    Transforms raw Airbyte telemetry data into clean, standardized records for analytics.
    
    SOURCE: telemetry_raw.telemetry_data (Airbyte ingestion)
    
    TRANSFORMATIONS:
    - Standardizes engine_id format and numeric precision (8,3)
    - Calculates fuel_efficiency_ratio and performance_score (0-100 weighted composite)
    - Flags anomalies outside operational parameters:
        * Pressure: 80-400 psi | Fuel: 15-220 kg/s | Temp: 1200-4500°F
    - Preserves Airbyte metadata and adds processing timestamps
    
    OUTPUT: Clean records for star schema modeling and anomaly detection dashboards
*/

{{ config(
    materialized='view',
    schema='staging'
) }}

WITH source_data AS (
    SELECT 
        -- Airbyte metadata
        _airbyte_raw_id,
        _airbyte_extracted_at,
        _airbyte_generation_id,
        
        -- Business data
        engine_id,
        timestamp::timestamp AS reading_timestamp,
        chamber_pressure::numeric(8,3) AS chamber_pressure_psi,
        fuel_flow::numeric(8,3) AS fuel_flow_kg_per_sec,
        temperature::numeric(8,3) AS temperature_fahrenheit
        
    FROM {{ source('telemetry_raw', 'telemetry_data') }}
),

cleaned_data AS (
    SELECT 
        _airbyte_raw_id,
        _airbyte_extracted_at,
        _airbyte_generation_id,
        
        -- Standardized business fields
        UPPER(TRIM(engine_id)) AS engine_id,
        reading_timestamp,
        chamber_pressure_psi,
        fuel_flow_kg_per_sec,
        temperature_fahrenheit,
        
        -- Calculated metrics
        CASE 
            WHEN chamber_pressure_psi > 0 
            THEN fuel_flow_kg_per_sec / chamber_pressure_psi 
            ELSE NULL 
        END AS fuel_efficiency_ratio,
        
        -- Composite performance score (weighted average) - relaxed thresholds
        (
            CASE WHEN chamber_pressure_psi BETWEEN 120 AND 350 THEN 0.4 ELSE 0.0 END +
            CASE WHEN fuel_flow_kg_per_sec BETWEEN 30 AND 180 THEN 0.3 ELSE 0.0 END +
            CASE WHEN temperature_fahrenheit BETWEEN 1800 AND 4200 THEN 0.3 ELSE 0.0 END
        ) * 100 AS performance_score,
        
        -- Data quality flags - more realistic thresholds
        CASE 
            WHEN chamber_pressure_psi < 80 OR chamber_pressure_psi > 400 THEN TRUE
            WHEN fuel_flow_kg_per_sec < 15 OR fuel_flow_kg_per_sec > 220 THEN TRUE
            WHEN temperature_fahrenheit < 1200 OR temperature_fahrenheit > 4500 THEN TRUE
            ELSE FALSE
        END AS is_anomaly,
        
        -- Anomaly type classification - relaxed thresholds
        CASE 
            WHEN chamber_pressure_psi < 80 THEN 'LOW_PRESSURE'
            WHEN chamber_pressure_psi > 400 THEN 'HIGH_PRESSURE'
            WHEN fuel_flow_kg_per_sec < 15 THEN 'LOW_FUEL_FLOW'
            WHEN fuel_flow_kg_per_sec > 220 THEN 'HIGH_FUEL_FLOW'
            WHEN temperature_fahrenheit < 1200 THEN 'LOW_TEMPERATURE'
            WHEN temperature_fahrenheit > 4500 THEN 'HIGH_TEMPERATURE'
            ELSE NULL
        END AS anomaly_type,
        
        -- ETL metadata
        'AIRBYTE' AS source_system,
        GETDATE() AS processed_at
        
    FROM source_data
    WHERE engine_id IS NOT NULL
      AND reading_timestamp IS NOT NULL
      AND chamber_pressure_psi IS NOT NULL
      AND fuel_flow_kg_per_sec IS NOT NULL
      AND temperature_fahrenheit IS NOT NULL
)

SELECT * FROM cleaned_data 