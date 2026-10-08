-- ==================================================
-- TELEMETRY ANALYTICS STAR SCHEMA DESIGN
-- ==================================================
-- Purpose: Transform raw telemetry data into analytics-ready star schema
-- Author: Data Engineering Demonstration Project
-- Date: 2025-06-30

-- ==================================================
-- FACT TABLE: Core Telemetry Measurements
-- ==================================================

CREATE TABLE telemetry_clean.fact_telemetry_readings (
    -- Surrogate Key
    reading_key BIGINT IDENTITY(1,1) PRIMARY KEY,
    
    -- Foreign Keys (Dimension References)
    engine_key INTEGER NOT NULL,
    time_key INTEGER NOT NULL,
    
    -- Degenerate Dimensions (High Cardinality IDs)
    airbyte_raw_id VARCHAR(50),
    
    -- Facts/Measures (Numeric Values for Analytics)
    chamber_pressure_psi DECIMAL(8,3) NOT NULL,
    fuel_flow_kg_per_sec DECIMAL(8,3) NOT NULL, 
    temperature_fahrenheit DECIMAL(8,3) NOT NULL,
    
    -- Calculated Measures (Business Logic)
    fuel_efficiency_ratio DECIMAL(8,6), -- fuel_flow / chamber_pressure
    performance_score DECIMAL(8,3),     -- composite health metric
    
    -- Data Quality Flags
    is_anomaly BOOLEAN DEFAULT FALSE,
    anomaly_type VARCHAR(50),
    
    -- ETL Metadata
    source_system VARCHAR(20) DEFAULT 'AIRBYTE',
    created_at TIMESTAMP DEFAULT GETDATE(),
    updated_at TIMESTAMP DEFAULT GETDATE()
)
DISTSTYLE KEY 
DISTKEY (engine_key)  -- Distribute by engine for performance
SORTKEY (time_key, engine_key); -- Sort for time-series queries

-- ==================================================
-- DIMENSION TABLE: Engine Information
-- ==================================================

CREATE TABLE telemetry_clean.dim_engines (
    -- Surrogate Key
    engine_key INTEGER IDENTITY(1,1) PRIMARY KEY,
    
    -- Natural Key
    engine_id VARCHAR(10) NOT NULL UNIQUE,
    
    -- Descriptive Attributes
    engine_name VARCHAR(100),
    engine_type VARCHAR(50) DEFAULT 'Rocket Engine',
    manufacturer VARCHAR(100) DEFAULT 'Example Aerospace',
    
    -- Technical Specifications
    max_chamber_pressure_psi INTEGER DEFAULT 350,
    max_fuel_flow_kg_per_sec INTEGER DEFAULT 200,
    max_temperature_fahrenheit INTEGER DEFAULT 4000,
    
    -- Operational Metadata
    installation_date DATE,
    last_maintenance_date DATE,
    operational_status VARCHAR(20) DEFAULT 'ACTIVE',
    
    -- SCD Type 2 (Slowly Changing Dimensions)
    effective_from_date DATE DEFAULT CURRENT_DATE,
    effective_to_date DATE DEFAULT '2999-12-31',
    is_current BOOLEAN DEFAULT TRUE,
    
    -- ETL Metadata
    source_system VARCHAR(20) DEFAULT 'MANUAL',
    created_at TIMESTAMP DEFAULT GETDATE(),
    updated_at TIMESTAMP DEFAULT GETDATE()
)
DISTSTYLE ALL  -- Small dimension, replicate to all nodes
SORTKEY (engine_id);

-- ==================================================
-- DIMENSION TABLE: Telemetry Metrics Metadata
-- ==================================================

CREATE TABLE telemetry_clean.dim_telemetry_metrics (
    -- Surrogate Key
    metric_key INTEGER IDENTITY(1,1) PRIMARY KEY,
    
    -- Metric Definition
    metric_name VARCHAR(50) NOT NULL UNIQUE,
    metric_category VARCHAR(30) NOT NULL, -- 'PRESSURE', 'FLOW', 'TEMPERATURE'
    metric_description TEXT,
    
    -- Units and Ranges
    unit_of_measure VARCHAR(20) NOT NULL,
    normal_min_value DECIMAL(8,3),
    normal_max_value DECIMAL(8,3),
    critical_min_value DECIMAL(8,3),
    critical_max_value DECIMAL(8,3),
    
    -- Quality Rules
    precision_digits INTEGER DEFAULT 3,
    data_quality_rules TEXT,
    
    -- ETL Metadata
    created_at TIMESTAMP DEFAULT GETDATE(),
    updated_at TIMESTAMP DEFAULT GETDATE()
)
DISTSTYLE ALL  -- Small dimension, replicate to all nodes
SORTKEY (metric_name);

-- ==================================================
-- BRIDGE TABLE: For Many-to-Many Relationships
-- ==================================================

CREATE TABLE telemetry_clean.bridge_engine_metrics (
    engine_key INTEGER NOT NULL,
    metric_key INTEGER NOT NULL,
    calibration_factor DECIMAL(8,6) DEFAULT 1.0,
    sensor_id VARCHAR(20),
    PRIMARY KEY (engine_key, metric_key)
)
DISTSTYLE KEY
DISTKEY (engine_key)
SORTKEY (engine_key, metric_key);

-- ==================================================
-- STAR SCHEMA INDEXES FOR PERFORMANCE
-- ==================================================

-- Fact Table Indexes (Redshift uses SORTKEY instead of indexes)
-- Already defined in table creation above

-- ==================================================
-- COMMENTS FOR DOCUMENTATION
-- ==================================================

COMMENT ON TABLE telemetry_clean.fact_telemetry_readings IS 
'Core fact table containing telemetry measurements with foreign keys to dimensions';

COMMENT ON TABLE telemetry_clean.dim_engines IS 
'Engine dimension with technical specifications and SCD Type 2 support';

COMMENT ON TABLE telemetry_clean.dim_telemetry_metrics IS 
'Metadata dimension for telemetry metrics with quality rules and ranges';

-- ==================================================
-- SAMPLE BUSINESS QUERIES THE SCHEMA SUPPORTS
-- ==================================================

/*
-- Q1: Engine Performance Comparison
SELECT 
    e.engine_id,
    AVG(f.chamber_pressure_psi) as avg_pressure,
    AVG(f.fuel_efficiency_ratio) as avg_efficiency,
    COUNT(*) as total_readings
FROM fact_telemetry_readings f
JOIN dim_engines e ON f.engine_key = e.engine_key
GROUP BY e.engine_id;

-- Q2: Time-based Anomaly Analysis (using reading_timestamp directly)
SELECT 
    DATE(f.reading_timestamp) as date_actual,
    EXTRACT(HOUR FROM f.reading_timestamp) as hour_24,
    COUNT(CASE WHEN f.is_anomaly THEN 1 END) as anomaly_count,
    COUNT(*) as total_readings
FROM fact_telemetry_readings f
GROUP BY DATE(f.reading_timestamp), EXTRACT(HOUR FROM f.reading_timestamp)
ORDER BY anomaly_count DESC;

-- Q3: Multi-dimensional Analysis (simplified without time dimension)
SELECT 
    e.engine_id,
    DATE(f.reading_timestamp) as reading_date,
    m.metric_category,
    AVG(CASE WHEN m.metric_name = 'chamber_pressure' 
        THEN f.chamber_pressure_psi END) as avg_pressure,
    AVG(CASE WHEN m.metric_name = 'fuel_flow' 
        THEN f.fuel_flow_kg_per_sec END) as avg_fuel_flow
FROM fact_telemetry_readings f
JOIN dim_engines e ON f.engine_key = e.engine_key  
CROSS JOIN dim_telemetry_metrics m
GROUP BY e.engine_id, DATE(f.reading_timestamp), m.metric_category;
*/ 