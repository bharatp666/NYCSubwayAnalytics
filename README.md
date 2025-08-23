# NYC Subway Ridership ELT Pipeline

This project implements an **end-to-end ELT pipeline** for analyzing **NYC Subway ridership data**.

Raw data is extracted from the [NY Open Data](https://data.ny.gov/Transportation/MTA-Subway-Hourly-Ridership-2020-2024/wujg-7c2s/about_data) using **Cloud Run**, staged in **Google Cloud Storage (GCS)**, processed with **Dataproc (Spark + Delta Lake)**, and loaded in **BigQuery**. The curated tables then power a **Looker Studio dashboard**, delivering insights into ridership patterns.





