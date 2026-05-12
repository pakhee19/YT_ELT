from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extras import RealDictCursor

table="yt_api" 



def get_cursor():
    hook = PostgresHook(postgres_conn_id="postgres_db_yt_elt", database="elt_db")

    conn = hook.get_conn()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    return conn, cur

def close_connection(conn, cur):
    cur.close()
    conn.close()    
    
def create_schema(schema):
    conn, cur = get_cursor()
    schema_sql = f"CREATE SCHEMA IF NOT EXISTS {schema};"
    cur.execute(schema_sql)
    conn.commit()
    close_connection(conn, cur)

def create_table(schema, table):
    conn, cur = get_cursor()
    if schema:
        table_sql = f"""
        CREATE TABLE IF NOT EXISTS {schema}.{table} (
            "video_id" VARCHAR(11) PRIMARY KEY NOT NULL,
            "title" TEXT NOT NULL,
            "upload_date" TIMESTAMP NOT NULL,
            "duration" VARCHAR(20) NOT NULL,
            "video_views" INT,
            "likes_count" INT,
            "comments_count" INT,
            "video_type" VARCHAR(10)
        );
        """

    else:
        table_sql = f"""
        CREATE TABLE IF NOT EXISTS {schema}.{table} (
            "video_id" VARCHAR(11) PRIMARY KEY NOT NULL,
            "title" TEXT NOT NULL,
            "upload_date" TIMESTAMP NOT NULL,
            "duration" VARCHAR(20) NOT NULL,
            "video_views" INT,
            "likes_count" INT,
            "comments_count" INT
        );
        """
    cur.execute(table_sql)
    conn.commit()   
    close_connection(conn, cur)

def get_video_ids(cur,schema):
    cur.execute(f"SELECT video_id FROM {schema}.{table};")
    ids = cur.fetchall()
    video_ids = [row['video_id'] for row in ids]     
    return video_ids