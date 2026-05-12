from datawarehouse.data_utils import get_cursor, close_connection, create_schema, create_table, get_video_ids
from datawarehouse.data_loading import load_path    
from datawarehouse.data_modification import update_rows, insert_rows, delete_rows

from datawarehouse.data_transformation import transform_data

import logging
from airflow.decorators import task
logger = logging.getLogger(__name__)
table="yt_api"

@task
def staging_table():
    schema='staging'
    conn,cur=None,None
    try:
        conn, cur = get_cursor()
        YT_data=load_path()
        create_schema(schema)
        create_table(schema, table)

        table_ids=get_video_ids(cur, schema)
        for row in YT_data:
            if len(table_ids)==0:
                insert_rows(cur, conn, schema, row)
            else:
                if row['video_id'] in table_ids:
                    update_rows(cur, conn, schema, row)
                else:
                    insert_rows(cur, conn, schema, row)
        ids_in_json = {row['video_id'] for row in YT_data}
        
        ids_to_delete = set(table_ids) - ids_in_json
        
        if ids_to_delete:
            delete_rows(cur, conn, schema, ids_to_delete)
        logger.info(f"{schema} table updated completed")
    except Exception as e:
        logger.error(f"An Error occured during the update of {schema} table: {e}")
        raise e

    finally :
        if conn and cur:
            close_connection(conn, cur) 

@task
def core_table():
    schema='core'
    conn,cur=None,None
    try:
        conn, cur = get_cursor()
        create_schema(schema)
        create_table(schema, table)

        table_ids=get_video_ids(cur, schema)

        current_video_ids = set()

        cur.execute(f"SELECT * FROM staging.{table};")
        rows=cur.fetchall()
        for row in rows:
            current_video_ids.add(row['video_id'])
            if len(table_ids)==0:
                transformed_row=transform_data(row)
                insert_rows(cur, conn, schema, transformed_row)
            else:
                transformed_row=transform_data(row)

                if transformed_row['video_id'] in table_ids:
                    update_rows(cur, conn, schema, transformed_row)
                else:
                    insert_rows(cur, conn, schema, transformed_row)
        ids_to_delete = set(table_ids) - current_video_ids

        if ids_to_delete:
            delete_rows(cur, conn, schema, ids_to_delete)   
        logger.info(f"{schema} table updated completed")
    except Exception as e:
        logger.error(f"An Error occured during the update of {schema} table: {e}")
        raise e
    finally :
        if conn and cur:
            close_connection(conn, cur)