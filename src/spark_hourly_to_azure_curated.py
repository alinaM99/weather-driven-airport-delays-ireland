import os
import glob
import shutil
from datetime import datetime, timezone

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from azure.storage.blob import BlobServiceClient
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, explode, from_unixtime, to_timestamp, lit,
    rand, floor, greatest, least,
    when, pmod, xxhash64
)

# =========================
# CONFIG 
# =========================
AZURE_CONNECTION_STRING = (
    "DefaultEndpointsProtocol=https;"
    "AccountName=alinadissstorage;"
    "AccountKey=xyz"
    "EndpointSuffix=core.windows.net"
)

CONTAINER_NAME = "weather-raw"

# Raw JSON location in Azure
RAW_PREFIX = "raw/onecall/"

# Curated Parquet location in Azure
CURATED_PREFIX = "curated/weather_hourly_big_parquet/"

# Final dataset size
TARGET_ROWS = 100000

# Local temp folders
LOCAL_RAW_DIR = "tmp_raw_json"
LOCAL_OUT_DIR = "tmp_curated_out"


# =========================
# HELPERS
# =========================
def clean_dir(path: str):
    if os.path.exists(path):
        shutil.rmtree(path)
    os.makedirs(path, exist_ok=True)


def download_json_blobs(container_client, prefix: str, local_dir: str) -> int:
    """
    Downloads all JSON blobs under prefix into local_dir.
    """
    clean_dir(local_dir)
    count = 0

    for blob in container_client.list_blobs(name_starts_with=prefix):
        name = blob.name
        if not name.lower().endswith(".json"):
            continue

        data = container_client.download_blob(name).readall()
        if not data:
            continue

        safe_name = name.replace("/", "__")
        out_path = os.path.join(local_dir, safe_name)
        with open(out_path, "wb") as f:
            f.write(data)

        count += 1

    return count


def upload_file(container_client, local_path: str, blob_name: str):
    with open(local_path, "rb") as f:
        container_client.upload_blob(blob_name, f, overwrite=True)


# =========================
# MAIN
# =========================
def main():
    # 1) Connect to Azure
    blob_service = BlobServiceClient.from_connection_string(AZURE_CONNECTION_STRING)
    container_client = blob_service.get_container_client(CONTAINER_NAME)

    # 2) Download raw JSON from Azure -> local
    n = download_json_blobs(container_client, RAW_PREFIX, LOCAL_RAW_DIR)
    print(f"Downloaded {n} JSON files to {LOCAL_RAW_DIR}")

    if n == 0:
        print("No JSON files found in Azure under RAW_PREFIX. Check RAW_PREFIX.")
        return

    # 3) Start Spark (Windows-stable)
    spark = SparkSession.builder \
        .appName("weather-hourly-curation-airport-delays") \
        .master("local[2]") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    # 4) Read JSON using explicit file paths (Windows-stable)
    files = glob.glob(os.path.join(LOCAL_RAW_DIR, "*"))
    files = [os.path.abspath(p) for p in files]

    print(f"Reading {len(files)} JSON files with Spark...")
    df = spark.read.option("multiLine", True).json(files)

    # 5) Explode hourly array (REAL API hourly rows)
    df_hourly = (
        df.select(
            col("lat").alias("lat"),
            col("lon").alias("lon"),
            col("timezone").alias("timezone"),
            explode(col("hourly")).alias("h")
        )
        .select(
            col("lat"),
            col("lon"),
            col("timezone"),
            col("h.dt").alias("dt_unix"),
            col("h.temp").alias("temp_c"),
            col("h.feels_like").alias("feels_like_c"),
            col("h.humidity").alias("humidity"),
            col("h.wind_speed").alias("wind_speed"),
            col("h.clouds").alias("clouds"),
            col("h.weather")[0]["main"].alias("weather_main"),
            col("h.weather")[0]["description"].alias("weather_description"),
        )
        .withColumn("obs_time_utc", to_timestamp(from_unixtime(col("dt_unix"))))
        .drop("dt_unix")
        .withColumn("data_source", lit("real_api"))
    )

    # ===== Airport mapping (based on your 4 points) =====
    df_hourly = df_hourly.withColumn(
        "airport_code",
        when((col("lat") == 53.4213) & (col("lon") == -6.2701), lit("DUB"))
        .when((col("lat") == 51.8413) & (col("lon") == -8.4911), lit("ORK"))
        .when((col("lat") == 52.6900) & (col("lon") == -8.9248), lit("SNN"))
        .when((col("lat") == 53.9103) & (col("lon") == -8.8185), lit("NOC"))
        .otherwise(lit("UNK"))
    )

    # Deterministic-ish noise per row (0..99) to keep "randomness" stable across runs
    noise_0_99 = pmod(xxhash64(col("airport_code"), col("obs_time_utc"), col("weather_main")), lit(100))

    # Weather severity score (0..100)
    df_hourly = df_hourly.withColumn(
        "severity_score",
        least(
            lit(100),
            greatest(
                lit(0),
                lit(5)
                + when(col("weather_main") == "Thunderstorm", lit(45))
                  .when(col("weather_main") == "Snow", lit(35))
                  .when(col("weather_main") == "Rain", lit(25))
                  .when(col("weather_main") == "Drizzle", lit(15))
                  .when(col("weather_main") == "Fog", lit(18))
                  .otherwise(lit(0))
                + when(col("wind_speed") >= 14, lit(30))
                  .when(col("wind_speed") >= 10, lit(18))
                  .when(col("wind_speed") >= 7,  lit(8))
                  .otherwise(lit(0))
                + when(col("clouds") >= 90, lit(10))
                  .when(col("clouds") >= 70, lit(5))
                  .otherwise(lit(0))
            )
        ).cast("int")
    )

    # Simulated delay minutes (transparent + realistic-ish)
    df_hourly = df_hourly.withColumn(
        "delay_minutes",
        greatest(
            lit(0),
            (
                floor(noise_0_99 / 8)          # 0..12 baseline
                + floor(col("severity_score") / 2)  # 0..50 from severity
            ).cast("int")
        )
    )

    df_hourly = df_hourly.withColumn("is_delayed", (col("delay_minutes") >= 15))

    df_hourly = df_hourly.withColumn(
        "delay_bucket",
        when(col("delay_minutes") < 15, lit("On-time"))
        .when(col("delay_minutes") < 30, lit("15–29"))
        .when(col("delay_minutes") < 60, lit("30–59"))
        .otherwise(lit("60+"))
    )

    df_hourly = df_hourly.withColumn("delay_source", lit("simulated"))

    real_count = df_hourly.count()
    print(f" Spark created REAL hourly rows (with airport columns): {real_count}")
    df_hourly.show(3, truncate=False)

    # 6) Expand to TARGET_ROWS (synthetic stress rows, clearly labeled)
    if real_count < TARGET_ROWS:
        multiplier = (TARGET_ROWS // real_count) + 1
        print(f"Creating synthetic rows to reach {TARGET_ROWS}. Multiplier={multiplier}")

        reps = spark.range(0, multiplier).withColumnRenamed("id", "rep_id")

        df_synth = (
            df_hourly.crossJoin(reps)
            .withColumn("data_source", lit("synthetic"))
            # jitter some numeric fields slightly
            .withColumn("temp_c", col("temp_c") + (rand() * 4 - 2))
            .withColumn("feels_like_c", col("feels_like_c") + (rand() * 4 - 2))
            .withColumn(
                "humidity",
                least(lit(100), greatest(lit(0), (col("humidity") + floor(rand() * 21 - 10)).cast("int")))
            )
            .withColumn("wind_speed", greatest(lit(0.0), col("wind_speed") + (rand() * 6 - 3)))
            .withColumn(
                "clouds",
                least(lit(100), greatest(lit(0), (col("clouds") + floor(rand() * 31 - 15)).cast("int")))
            )
            # recompute severity + delay for the jittered weather
            .withColumn(
                "severity_score",
                least(
                    lit(100),
                    greatest(
                        lit(0),
                        lit(5)
                        + when(col("weather_main") == "Thunderstorm", lit(45))
                          .when(col("weather_main") == "Snow", lit(35))
                          .when(col("weather_main") == "Rain", lit(25))
                          .when(col("weather_main") == "Drizzle", lit(15))
                          .when(col("weather_main") == "Fog", lit(18))
                          .otherwise(lit(0))
                        + when(col("wind_speed") >= 14, lit(30))
                          .when(col("wind_speed") >= 10, lit(18))
                          .when(col("wind_speed") >= 7,  lit(8))
                          .otherwise(lit(0))
                        + when(col("clouds") >= 90, lit(10))
                          .when(col("clouds") >= 70, lit(5))
                          .otherwise(lit(0))
                    )
                ).cast("int")
            )
            .withColumn(
                "delay_minutes",
                greatest(
                    lit(0),
                    (
                        floor(noise_0_99 / 8)
                        + floor(col("severity_score") / 2)
                    ).cast("int")
                )
            )
            .withColumn("is_delayed", (col("delay_minutes") >= 15))
            .withColumn(
                "delay_bucket",
                when(col("delay_minutes") < 15, lit("On-time"))
                .when(col("delay_minutes") < 30, lit("15–29"))
                .when(col("delay_minutes") < 60, lit("30–59"))
                .otherwise(lit("60+"))
            )
            .drop("rep_id")
        )

        df_out = df_hourly.unionByName(df_synth).limit(TARGET_ROWS)
    else:
        df_out = df_hourly.limit(TARGET_ROWS)

    out_count = df_out.count()
    print(f" Final dataset rows: {out_count}")

    # 7) Write ONE parquet locally using pyarrow 
    clean_dir(LOCAL_OUT_DIR)
    out_path = os.path.join(LOCAL_OUT_DIR, "weather_hourly_big.parquet")

    pdf = df_out.toPandas()
    table = pa.Table.from_pandas(pdf)
    pq.write_table(table, out_path)

    print(f" Wrote Parquet locally: {out_path}")

    # 8) Upload curated parquet to Azure
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    blob_name = f"{CURATED_PREFIX}run_ts={ts}/weather_hourly_big.parquet"
    upload_file(container_client, out_path, blob_name)

    print("\n DONE: Spark created HOURLY dataset (with airport delays) and uploaded curated Parquet to Azure.")
    print("Azure curated file:", blob_name)
    print("Note: data_source marks real_api vs synthetic. delay_source=simulated for delay fields.")

    spark.stop()


if __name__ == "__main__":
    main()
