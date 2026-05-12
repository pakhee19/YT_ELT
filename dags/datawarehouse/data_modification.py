import logging

logger = logging.getLogger(__name__)
table = "yt_api"


def insert_rows(cur, conn, schema, row):
    video_id = 'video_id'

    try:
        if schema == "staging":
            cur.execute(
                f"""
                INSERT INTO {schema}.{table}
                (
                    video_id,
                    title,
                    upload_date,
                    duration,
                    video_views,
                    likes_count,
                    comments_count
                )
                VALUES
                (
                    %(video_id)s,
                    %(title)s,
                    %(publishedAt)s,
                    %(duration)s,
                    %(viewCount)s,
                    %(likeCount)s,
                    %(commentCount)s
                )

                ON CONFLICT (video_id)

                DO UPDATE SET
                    title = EXCLUDED.title,
                    upload_date = EXCLUDED.upload_date,
                    duration = EXCLUDED.duration,
                    video_views = EXCLUDED.video_views,
                    likes_count = EXCLUDED.likes_count,
                    comments_count = EXCLUDED.comments_count;
                """,
                row
            )

        else:
            cur.execute(
                f"""
                INSERT INTO {schema}.{table}
                (
                    video_id,
                    title,
                    upload_date,
                    duration,
                    video_views,
                    likes_count,
                    comments_count,
                    video_type
                )
                VALUES
                (
                    %(video_id)s,
                    %(title)s,
                    %(upload_date)s,
                    %(duration)s,
                    %(video_views)s,
                    %(likes_count)s,
                    %(comments_count)s,
                    %(Video_Type)s
                )

                ON CONFLICT (video_id)

                DO UPDATE SET
                    title = EXCLUDED.title,
                    upload_date = EXCLUDED.upload_date,
                    duration = EXCLUDED.duration,
                    video_views = EXCLUDED.video_views,
                    likes_count = EXCLUDED.likes_count,
                    comments_count = EXCLUDED.comments_count,
                    video_type = EXCLUDED.video_type;
                """,
                row
            )

        conn.commit()
        logger.info(f"Inserted/Updated video_id: {row[video_id]}")

    except Exception as e:
        logger.error(f"Error inserting video_id: {row[video_id]} - {e}")
        raise e

def update_rows(cur, conn, schema, row):
    video_id = 'video_id'

    try:
        if schema == "staging":
            cur.execute(
                f"""
                UPDATE {schema}.{table}
                SET
                    title = %(title)s,
                    video_views = %(viewCount)s,
                    likes_count = %(likeCount)s,
                    comments_count = %(commentCount)s
                WHERE video_id = %(video_id)s;
                """,
                row
            )

        else:
            cur.execute(
                f"""
                UPDATE {schema}.{table}
                SET
                    title = %(title)s,
                    video_views = %(video_views)s,
                    likes_count = %(likes_count)s,
                    comments_count = %(comments_count)s
                WHERE video_id = %(video_id)s;
                """,
                row
            )

        conn.commit()
        logger.info(f"Updated video_id: {row[video_id]}")

    except Exception as e:
        logger.error(f"Error updating video_id: {row[video_id]} - {e}")
        raise e


def delete_rows(cur, conn, schema, ids_to_delete):

    try:
        ids_to_delete = f"""({','.join(f"'{id}'" for id in ids_to_delete)})"""

        cur.execute(
            f"""
            DELETE FROM {schema}.{table}
            WHERE video_id IN {ids_to_delete};
            """
        )

        conn.commit()

        logger.info(f"Deleted video_ids: {ids_to_delete}")

    except Exception as e:
        logger.error(f"Error deleting video_ids: {ids_to_delete} - {e}")
        raise e