import pandas as pd


def query_file(path):
    with open(path) as f:
        query = f.read()
    return query


def pg_to_df(query, pg_hook):
    conn = pg_hook.get_conn()

    try:
        return pd.read_sql_query(query, conn)
    except Exception as error:
        raise("Error fetching data from PostgreSQL:", error)
    finally:
        if conn:
            conn.close()