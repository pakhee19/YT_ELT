import pytest
import requests
import os
import psycopg2
def test_youtube_api_response(airflow_variable):
    api_key = airflow_variable("API_KEY")
    channel_handle = airflow_variable("CHANNEL_HANDLE")

    url = f"https://youtube.googleapis.com/youtube/v3/channels?part=contentDetails&forHandle={channel_handle}&key={api_key}"
    
    try:
        response = requests.get(url)
        assert response.status_code == 200
    except requests.RequestException as e:
        pytest.fail(f"API request failed: {e}")
@pytest.fixture()
def real_postgres_connection():

    required_vars = {
    "ELT_DATABASE_NAME": os.getenv("ELT_DATABASE_NAME"),
    "ELT_DATABASE_USERNAME": os.getenv("ELT_DATABASE_USERNAME"),
    "ELT_DATABASE_PASSWORD": os.getenv("ELT_DATABASE_PASSWORD"),
    "POSTGRES_CONN_HOST": os.getenv("POSTGRES_CONN_HOST"),
    "POSTGRES_CONN_PORT": os.getenv("POSTGRES_CONN_PORT"),
}

    missing = [k for k, v in required_vars.items() if not v]

    if missing:
        pytest.fail(f"Missing environment variables: {missing}")

    conn = psycopg2.connect(
    dbname=required_vars["ELT_DATABASE_NAME"],
    user=required_vars["ELT_DATABASE_USERNAME"],
    password=required_vars["ELT_DATABASE_PASSWORD"],
    host=required_vars["POSTGRES_CONN_HOST"],
    port=required_vars["POSTGRES_CONN_PORT"],
)

    yield conn

    conn.close()

def test_real_postgres_connection(real_postgres_connection):
    cursor = real_postgres_connection.cursor()

    cursor.execute("SELECT 1;")

    result = cursor.fetchone()

    assert result[0] == 1

    cursor.close()