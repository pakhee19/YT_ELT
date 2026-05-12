import logging
from airflow.operators.bash import BashOperator

logger = logging.getLogger(__name__)

SODA_PATH = "/opt/airflow/include/soda"
DATASOURCE = "pg_datasource"

def yt_api_data_quality_check(schema):
    try:
        task = BashOperator(
            task_id=f'soda_check_{schema}',
            bash_command=f"""
cat > /tmp/checks_{schema}.yml << 'EOF'
checks for {schema}.yt_api:
  - missing_count("video_id") = 0
  - duplicate_count("video_id") = 0

  - likes_count_greater_than_vid_views = 0:
      name: Check for likes_count greater than video_views
      likes_count_greater_than_vid_views_query: |
        SELECT COUNT(*)
        FROM {schema}.yt_api
        WHERE likes_count > video_views

  - comments_count_greater_than_vid_views = 0:
      name: Check for comments_count greater than video_views
      comments_count_greater_than_vid_views_query: |
        SELECT COUNT(*)
        FROM {schema}.yt_api
        WHERE comments_count > video_views
EOF
soda scan -d {DATASOURCE} -c {SODA_PATH}/configuration.yml /tmp/checks_{schema}.yml
"""
        )
        return task
    except Exception as e:
        logger.error(f"Error running data quality check for schema: {schema}: {e}")
        raise e