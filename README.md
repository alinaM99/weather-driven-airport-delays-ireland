# weather-driven-airport-delays-ireland
End-to-end scalable data pipeline analysing weather-driven airport delays in Ireland. Ingests OpenWeather API data, processes and scales it using Apache Spark, stores curated data in PostgreSQL, and visualises insights with Power BI.

## Overview
This project implements an end-to-end data engineering and analytics pipeline to examine how weather conditions impact airport delay patterns across major Irish airports.

## Architecture
Hourly weather data is ingested from the OpenWeather API and stored as raw JSON in cloud object storage. Apache Spark is used to transform and curate the data into an analytical dataset, which is loaded into PostgreSQL and visualised using Power BI.

## Data Pipeline
- API ingestion using Python
- Raw data storage in Azure Blob Storage
- Distributed transformation and scaling using Apache Spark
- Analytical serving layer in PostgreSQL
- Interactive dashboards in Power BI

## Analytics & Visualisation
The dashboards explore delay distributions by airport, cumulative delays by weather conditions, and relationships between wind intensity and delay severity.

## Technologies Used
Python, Apache Spark, Azure Blob Storage, PostgreSQL, SQL, Power BI, OpenWeather API
