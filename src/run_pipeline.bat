@echo off
cd /d "C:\Users\alina\OneDrive\Desktop\data_intensive_artifact_submission\src"

REM 1) Ingest API -> Azure Raw
py -3.10 openweatherapi_integration.py

REM 2) Spark curate -> Azure Curated
py -3.10 spark_hourly_to_azure_curated.py

REM 3) Curated -> Postgres
py -3.10 curated_parquet_to_postgres.py

echo DONE %date% %time%
