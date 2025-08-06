#govdata‑stream‑etl (Kafka → PySpark → S3 → Snowflake → Power BI)
Engineered real‑time ETL pipeline using Kafka, PySpark, and AWS S3 for streaming government datasets.
Automated data loading into Snowflake via Snowpipe with secure cross-account IAM trust and external S3 stage.
Ensured clean ingestion by filtering out system files (_spark_metadata) using PATTERN and MATCH_BY_COLUMN_NAME mapping.
Built interactive dashboards in Power BI, visualizing processed data for clear stakeholder insights.
