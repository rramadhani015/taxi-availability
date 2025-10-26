from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from snowflake.connector.pandas_tools import write_pandas

import pandas as pd
import hashlib
import uuid
import requests
from datetime import datetime
import time

# ========== CONFIG ==========
POSTGRES_CONN_ID = "pg1"
SNOWFLAKE_CONN_ID = "sf1"

# ========== HELPER ==========
def load_to_snowflake(table_name, df):
    sf_hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    engine = sf_hook.get_sqlalchemy_engine()
    df.to_sql(table_name, con=engine, index=False, if_exists='append')

# ========== TASK 1: General Table ==========
def insert_taxi_general(**kwargs):
    context = kwargs or {}
    # --- Extract from Postgres ---
    pg = PostgresHook(postgres_conn_id="pg1")
    query = "SELECT * FROM public.taxi_availability"
    df = pg.get_pandas_df(query)

    sf = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    # --- Iterate and insert ---
    insert_sqls = []
    count=0
    for _, row in df.iterrows():
        count=count+1
        dwh_id = str(uuid.uuid4())
        source_id = str(row["id"])
        captured_at = row["properties_timestamp"]
        total_taxis = row["properties_taxi_count"]
        source_system = "POSTGRES"
        hash_key = hashlib.md5(source_id.encode("utf-8")).hexdigest()
        load_batch_id = context['run_id']
        record_status = "ACTIVE"
        insert_sqls.append(f"""('{dwh_id}', '{source_id}', '{captured_at}', {total_taxis}, '{source_system}', '{hash_key}', '{load_batch_id}', '{record_status}')""")

    # Join all rows into a single INSERT
    values_str = ',\n'.join(insert_sqls)

    sql = (
        "INSERT INTO TAXI_AVAILABILITY ("
        "DWH_ID, SOURCE_ID, CAPTURED_AT, TOTAL_TAXIS, "
        "SOURCE_SYSTEM, HASH_KEY, LOAD_BATCH_ID, RECORD_STATUS"
        ") VALUES \n"
        f"{values_str};"
    )

    # --- Execute all inserts in batch --
    sf.run(sql)
    print(f"✅ Inserted {count} records to TAXI_AVAILABILITY (batch: {load_batch_id})")


# ========== TASK 2: Geometry Table ==========
def insert_taxi_geometry(**kwargs):
    context = kwargs or {}

    # --- Extract from Postgres ---
    pg = PostgresHook(postgres_conn_id="pg1")
    query = "SELECT id, geometry_coordinates FROM public.taxi_availability"
    df = pg.get_pandas_df(query)

    sf = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)

    # --- Prepare insert rows ---
    insert_sqls = []
    count = 0
    load_batch_id = str(context['run_id'])  # Ensure it's a string

    for _, row in df.iterrows():
        source_id = str(row["id"]) 
        coords_list = row["geometry_coordinates"]

        for coord in coords_list:
            count += 1
            dwh_id = str(uuid.uuid4())
            longitude = float(coord[0])
            latitude = float(coord[1])
            record_status = "ACTIVE"

            # Escape function
            def esc(val):
                if isinstance(val, str):
                    return val.replace("'", "''")
                return val

            insert_sqls.append(
                "('{dwh_id}', '{source_id}', {longitude}, {latitude}, '{load_batch_id}', '{record_status}')".format(
                    dwh_id=esc(dwh_id),
                    source_id=esc(source_id),
                    longitude=longitude,
                    latitude=latitude,
                    load_batch_id=esc(load_batch_id),
                    record_status=esc(record_status)
                )
            )

    # --- Insert in batches of 50 rows ---
    batch_size = 50000
    for i in range(0, len(insert_sqls), batch_size):
        batch_rows = insert_sqls[i:i + batch_size]
        values_str = ",\n".join(batch_rows)

        sql = (
            "INSERT INTO TAXI_GEOMETRY ("
            "DWH_ID, SOURCE_ID, LONGITUDE, LATITUDE, LOCATION, LOAD_BATCH_ID, RECORD_STATUS"
            ") "  
            "SELECT DWH_ID, SOURCE_ID, LONGITUDE, LATITUDE, ST_MAKEPOINT(LONGITUDE, LATITUDE) as LOCATION, LOAD_BATCH_ID, RECORD_STATUS FROM"
            " VALUES \n"
            f"{values_str}"
            "AS t(DWH_ID, SOURCE_ID, LONGITUDE, LATITUDE, LOAD_BATCH_ID, RECORD_STATUS);"
        )
        sf.run(sql)
        print(f"✅ Inserted rows {i+1} to {i+len(batch_rows)} (batch: {load_batch_id})")

    print(f"🎉 Total inserted: {count} records")

# ========== TASK 3: Reverse Geocode Table ==========
# scaling issue
# def insert_taxi_location(**kwargs):
#     context = kwargs or {}

#     sf_hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
#     engine = sf_hook.get_sqlalchemy_engine()

#     # Fetch coords not yet geocoded
#     query = """
#         SELECT g.DWH_ID AS geom_id, g.latitude, g.longitude
#         FROM TAXI_GEOMETRY g
#         WHERE geom_id not in (select geom_id from TAXI_LOCATION)
#         limit 5
#     """
#     df = pd.read_sql(query, engine)

#     rows = []
#     for _, r in df.iterrows():
#         url = f"https://nominatim.openstreetmap.org/reverse?format=jsonv2&lat={r.latitude}&lon={r.longitude}"
#         headers = {"User-Agent": "airflow-taxi-geocoder"}
#         resp = requests.get(url, headers=headers)
#         if resp.status_code == 200:
#             print("Getting location from OSM")
#             data = resp.json()
#             rows.append({
#                 "geom_id": r.geom_id,
#                 "country": data.get("address", {}).get("country", "Singapore"),  # default to Singapore
#                 "region": data.get("address", {}).get("suburb"),
#                 "city": data.get("address", {}).get("city_district"),
#                 "street": data.get("address", {}).get("road"),
#                 "postal_code": data.get("address", {}).get("postcode"),
#                 "location_name": data.get("display_name")  # optional, can store full display name
#             })
#         time.sleep(1)  # rate-limit protection

#     # --- Prepare insert rows ---
#     insert_sqls = []
#     count = 0
#     load_batch_id = str(context['run_id'])  # Ensure it's a string

#     def esc(val):
#         """Escape single quotes for SQL"""
#         if isinstance(val, str):
#             return val.replace("'", "''")
#         return val

#     for row in rows:
#         count += 1
#         dwh_id = str(uuid.uuid4())
#         geom_id = str(row["geom_id"])
#         country = row.get("country", "Singapore")
#         region = row.get("region")
#         city = row.get("city")
#         street = row.get("street")
#         postal_code = row.get("postal_code")
#         record_status = "ACTIVE"
#         print(postal_code)

#         insert_sqls.append(
#             f"('{esc(dwh_id)}', '{esc(geom_id)}', '{esc(country)}', '{esc(region)}', '{esc(city)}', '{esc(street)}', '{esc(postal_code)}', '{esc(load_batch_id)}', '{esc(record_status)}')"
#         )

#     # --- Insert in batches of 50 rows ---
#     batch_size = 50
#     for i in range(0, len(insert_sqls), batch_size):
#         batch_rows = insert_sqls[i:i + batch_size]
#         values_str = ",\n".join(batch_rows)

#         sql = f"""
#         INSERT INTO TAXI_LOCATION (DWH_ID, GEOM_ID, COUNTRY, REGION, CITY, STREET, POSTAL_CODE, LOAD_BATCH_ID, RECORD_STATUS)
#         VALUES
#             {values_str};
#         """

#         # sf_hook.run(sql)
#         print(sql)
#         print(f"✅ Inserted rows {i+1} to {i+len(batch_rows)} (batch: {load_batch_id})")

#     print(f"🎉 Total inserted: {count} records into TAXI_LOCATION")


