-- run in postgres
CREATE TABLE public.taxi_availability (
    id SERIAL PRIMARY KEY,
    type TEXT,
    crs_type TEXT,
    crs_properties_href TEXT,
    crs_properties_type TEXT,
    feature_type TEXT,
    geometry_type TEXT,
    geometry_coordinates JSONB,
    properties_timestamp TIMESTAMP,
    properties_taxi_count INTEGER,
    properties_api_info_status TEXT
);


-- run in snowflake
CREATE OR REPLACE TABLE TAXI_AVAILABILITY (
    DWH_ID STRING DEFAULT UUID_STRING() PRIMARY KEY,  -- surrogate key
    SOURCE_ID STRING,                                 -- original ID from API or staging
    CAPTURED_AT TIMESTAMP_NTZ,                        -- timestamp when data captured
    TOTAL_TAXIS INTEGER,
    SOURCE_SYSTEM STRING,                             -- e.g. LTA_API, POSTGRES
    INGESTED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP,
    HASH_KEY STRING,                                  -- optional MD5 for deduplication
    LOAD_BATCH_ID STRING,                             -- optional batch tracking
    RECORD_STATUS STRING DEFAULT 'ACTIVE'             -- e.g. ACTIVE/DELETED
);

CREATE OR REPLACE TABLE TAXI_GEOMETRY (
    DWH_ID STRING DEFAULT UUID_STRING() PRIMARY KEY,   -- surrogate key
    SOURCE_ID STRING NOT NULL,               -- FK to TAXI_AVAILABILITY.DWH_ID
    LONGITUDE FLOAT,
    LATITUDE FLOAT,
    LOCATION GEOGRAPHY,
    INGESTED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP,
    LOAD_BATCH_ID STRING,
    RECORD_STATUS STRING DEFAULT 'ACTIVE'
);

-- scaling issue
-- CREATE OR REPLACE TABLE TAXI_LOCATION (
--     DWH_ID STRING DEFAULT UUID_STRING() PRIMARY KEY,   -- surrogate key
--     GEOM_ID STRING NOT NULL,
--     COUNTRY STRING DEFAULT 'Singapore',
--     REGION STRING,
--     CITY STRING,
--     STREET STRING,
--     POSTAL_CODE STRING,
--     INGESTED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP,
--     LOAD_BATCH_ID STRING,
--     RECORD_STATUS STRING DEFAULT 'ACTIVE'
-- );

CREATE OR REPLACE TABLE DQ_LOG_TAXI (
    DWH_ID STRING DEFAULT UUID_STRING() PRIMARY KEY,  -- surrogate key for log
    TABLE_NAME STRING,                                -- table checked
    RUN_ID STRING,                                    -- Airflow run_id
    CHECK_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP,
    TOTAL_ROWS INTEGER,
    UNIQUE_DWH INTEGER,
    NULL_COUNT INTEGER,                                -- e.g., nulls in critical columns
    ORPHAN_ROWS INTEGER,                               -- if applicable
    ADDITIONAL_INFO VARIANT                            -- optional JSON for extra metrics
);
