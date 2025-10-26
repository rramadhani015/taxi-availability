from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime
import json
import uuid

SNOWFLAKE_CONN_ID = "sf1"

def dq_check_and_log(**kwargs):
    context = kwargs
    run_id = str(context['run_id'])
    sf = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)

    dq_logs = []

    # ---------------- TAXI_AVAILABILITY ----------------
    availability_sql = """
    SELECT 
        COUNT(*) AS total_rows,
        COUNT(DISTINCT DWH_ID) AS unique_dwh,
        SUM(CASE WHEN TOTAL_TAXIS IS NULL THEN 1 ELSE 0 END) AS null_total_taxis
    FROM TAXI_AVAILABILITY
    """
    df_avail = sf.get_pandas_df(availability_sql)

    dq_logs.append({
        "table_name": "TAXI_AVAILABILITY",
        "total_rows": df_avail['TOTAL_ROWS'][0],
        "unique_dwh": df_avail['UNIQUE_DWH'][0],
        "null_count": df_avail['NULL_TOTAL_TAXIS'][0],
        "orphan_rows": None
    })

    # ---------------- TAXI_GEOMETRY ----------------
    geometry_sql = """
    SELECT 
        COUNT(*) AS total_rows,
        COUNT(DISTINCT DWH_ID) AS unique_dwh,
        SUM(CASE WHEN LONGITUDE IS NULL OR LATITUDE IS NULL THEN 1 ELSE 0 END) AS null_coords,
        SUM(CASE WHEN SOURCE_ID NOT IN (SELECT DWH_ID FROM TAXI_AVAILABILITY) THEN 1 ELSE 0 END) AS orphan_rows
    FROM TAXI_GEOMETRY
    """
    df_geom = sf.get_pandas_df(geometry_sql)

    dq_logs.append({
        "table_name": "TAXI_GEOMETRY",
        "total_rows": df_avail['TOTAL_ROWS'][0],
        "unique_dwh": df_avail['UNIQUE_DWH'][0],
        "null_count": df_avail['NULL_TOTAL_TAXIS'][0],
        "orphan_rows": None
    })

    # ---------------- INSERT DQ LOGS ----------------
    insert_values = []
    for log in dq_logs:
        insert_values.append(f"""(
            '{str(uuid.uuid4())}',
            '{log["table_name"]}',
            '{run_id}',
            CURRENT_TIMESTAMP,
            {log["total_rows"]},
            {log["unique_dwh"]},
            {log["null_count"] if log["null_count"] is not None else 'NULL'},
            {log["orphan_rows"] if log["orphan_rows"] is not None else 'NULL'}
        )""")

    values_str = ",\n".join(insert_values)
    insert_sql = f"""
    INSERT INTO DQ_LOG_TAXI (
        DWH_ID, TABLE_NAME, RUN_ID, CHECK_TIMESTAMP,
        TOTAL_ROWS, UNIQUE_DWH, NULL_COUNT, ORPHAN_ROWS
    ) VALUES
    {values_str};
    """

    sf.run(insert_sql)
    print(f"✅ DQ logs inserted for run_id={run_id}")