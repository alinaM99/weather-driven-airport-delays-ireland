import io
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from azure.storage.blob import BlobServiceClient

# ===== Azure =====
AZURE_CONNECTION_STRING = (
    "DefaultEndpointsProtocol=https;"
    "AccountName=alinadissstorage;"
    "AccountKey=xyz"
    "EndpointSuffix=core.windows.net"
)
CONTAINER_NAME = "weather-raw"

# Paste the NEW Azure curated blob path from Spark output here
CURATED_PARQUET_BLOB = "curated/weather_hourly_big_parquet/run_ts=20251217_185657/weather_hourly_big.parquet"

# ===== Postgres =====
PG_HOST = "127.0.0.1"
PG_PORT = 5432
PG_DB = "weatherdb"
PG_USER = "postgres"
PG_PASSWORD = "AlinaM99!"

def main():
    blob_service = BlobServiceClient.from_connection_string(AZURE_CONNECTION_STRING)
    container_client = blob_service.get_container_client(CONTAINER_NAME)

    print("Downloading curated parquet from Azure...")
    data = container_client.download_blob(CURATED_PARQUET_BLOB).readall()
    print("Bytes downloaded:", len(data))

    df = pd.read_parquet(io.BytesIO(data))
    print("Rows in parquet:", len(df))
    print(df.head(3))

    conn = psycopg2.connect(
        host=PG_HOST, port=PG_PORT, dbname=PG_DB, user=PG_USER, password=PG_PASSWORD
    )
    conn.autocommit = False

    create_sql = """
    CREATE SCHEMA IF NOT EXISTS analytics;

    DROP TABLE IF EXISTS analytics.weather_hourly_big;

    CREATE TABLE analytics.weather_hourly_big (
      obs_time_utc TIMESTAMP,
      lat DOUBLE PRECISION,
      lon DOUBLE PRECISION,
      timezone TEXT,
      temp_c DOUBLE PRECISION,
      feels_like_c DOUBLE PRECISION,
      humidity INT,
      wind_speed DOUBLE PRECISION,
      clouds INT,
      weather_main TEXT,
      weather_description TEXT,
      data_source TEXT,
      airport_code TEXT,
      severity_score INT,
      delay_minutes INT,
      is_delayed BOOLEAN,
      delay_bucket TEXT,
      delay_source TEXT
    );
    """

    try:
        with conn.cursor() as cur:
            print("Creating table analytics.weather_hourly_big ...")
            cur.execute(create_sql)

            print("Inserting rows (fast batch insert)...")
            records = list(df[[
                "obs_time_utc","lat","lon","timezone","temp_c","feels_like_c","humidity",
                "wind_speed","clouds","weather_main","weather_description","data_source",
                "airport_code","severity_score","delay_minutes","is_delayed","delay_bucket","delay_source"
            ]].itertuples(index=False, name=None))

            insert_sql = """
            INSERT INTO analytics.weather_hourly_big (
              obs_time_utc, lat, lon, timezone, temp_c, feels_like_c, humidity,
              wind_speed, clouds, weather_main, weather_description, data_source,
              airport_code, severity_score, delay_minutes, is_delayed, delay_bucket, delay_source
            ) VALUES %s;
            """

            execute_values(cur, insert_sql, records, page_size=5000)

        conn.commit()
        print(" Done. Inserted:", len(df))

    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

if __name__ == "__main__":
    main()
