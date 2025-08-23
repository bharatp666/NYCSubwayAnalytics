# NYC Subway Ridership ELT Pipeline

## Overview
This project implements an **end-to-end ELT pipeline** for analyzing **NYC Subway ridership data**.

Raw data is extracted from the [NY Open Data](https://data.ny.gov/Transportation/MTA-Subway-Hourly-Ridership-2020-2024/wujg-7c2s/about_data) using **Cloud Run**, staged in **Google Cloud Storage (GCS)**, processed with **Dataproc (Spark + Delta Lake)**, and loaded in **BigQuery**. The curated tables then power a **Looker Studio dashboard**, delivering insights into ridership patterns.

## Architecture
![NYC Ridership Data Pipeline Architecture](DataPipeline.png)


## Features & Key Highlights

- **Automated Ingestion** → Fetches NYC Subway ridership data directly from the NY Open Data API using Cloud Run.  
- **Cloud-Native Storage** → Stores raw ingested data in Google Cloud Storage (GCS) for reliability and scalability.  
- **Distributed Processing** → Uses Dataproc (Spark + Delta Lake) to clean, transform, and structure large volumes of ridership data.  
- **Analytics Warehouse** → Loads curated datasets into BigQuery for fast, SQL-based analysis.  
- **Interactive Dashboard** → Powers a Looker Studio dashboard to visualize ridership patterns, transfers, and station performance.  
- **End-to-End Orchestration** → Fully managed with GCP services, minimizing manual intervention and ensuring scalability.


