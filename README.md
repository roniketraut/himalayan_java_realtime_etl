# Himalayan Java: End-to-End Real-Time Data Lakehouse

This project demonstrates a production-grade, real-time data platform built for a coffee house chain. It leverages a Medallion Architecture to transform raw POS event streams into actionable business insights, fully governed by Unity Catalog and orchestrated via Databricks Workflows.


## System Architecture
The platform follows a modern "Full-Stack" Data Engineering pattern:
Source: A containerized Python POS Simulator (Docker) generating mock sales events.
Ingestion: Real-time event streaming via Azure Event Hubs (Kafka API).
Processing: Spark Structured Streaming on Databricks handles the multi-stage ETL.
Governance: Unity Catalog provides centralized identity (Managed Identity), auditing, and lineage across three Azure Data Lake (ADLS Gen2) containers.
Storage: Delta Lake format ensures ACID transactions and Time Travel capabilities.
Orchestration: Databricks Workflows manages the end-to-end job dependency graph.
Analytics: Databricks SQL powers an Executive Dashboard with real-time sales KPIs.


## Tech Stack
Languages: Python, SQL, Bash
Data Platform: Azure Databricks (Runtime 16.4+)
Streaming: Azure Event Hubs, Spark Structured Streaming
Governance: Unity Catalog (Service Principals, Access Connectors, Volumes)
Storage: Azure Data Lake Storage Gen2 (ADLS), Delta Lake
DevOps/CI-CD: GitHub, Docker, Databricks Git Folders


## The Medallion Pipeline

### 1. Bronze Layer (Raw Ingestion)
Ingests raw JSON payloads from Event Hubs. I implemented a "Truly Raw" pattern, preserving Kafka metadata (offsets, partitions) to ensure zero data loss and full auditability.
Checkpointing: Managed via Unity Catalog Volumes for fault tolerance.

#### 2. Silver Layer (Transformation)
Performs schema enforcement and data normalization.
Parsing: Flattened nested JSON structures using from_json.
Explosion: Transformed data from Order-level to Item-level granularity using explode().
Optimization: Cast data types to proper Timestamps and Decimals for high-performance downstream querying.

#### 3. Gold Layer (Business Aggregations)
Creates high-level business summaries using Complete Output Mode.
Windowing: Implemented 1-hour Tumbling Windows with a 10-minute Watermark to handle late-arriving data.
Insights: Generated Branch Revenue, Product Popularity, and Hourly Sales Trends.

## Orchestration & Governance

### Databricks Workflow Job
I designed a multi-task DAG (Directed Acyclic Graph) that orchestrates the entire lifecycle:
Sequential ETL: Bronze → Silver → Gold tasks ensure data integrity.
Fan-out Pattern: The Gold layer branches out into three parallel SQL tasks to update specific business summaries simultaneously.
Automatic Refresh: The final task triggers a refresh of the Executive Dashboard, ensuring the business always sees the latest data.


<img width="2112" height="670" alt="Screenshot 2026-01-10 130904" src="https://github.com/user-attachments/assets/46c02b8a-a326-432e-9cd8-e9fda715e6e6" />


### Unity Catalog Implementation
Security: Implemented a "Keyless" architecture using an Azure Access Connector (Managed Identity).
Isolation: Physically separated data across raw, processed, and transformed containers using Schema-level Managed Locations.
Lineage: Leveraged UC to provide automated end-to-end data lineage, traceable from the SQL Dashboard back to the raw Event Hubs stream.

<img width="1206" height="792" alt="unity catalog lineage" src="https://github.com/user-attachments/assets/4b9cf87b-832b-45b0-9f47-037de178fd69" />


## Deployment (Docker)
The POS Simulator is containerized to ensure environment parity and portability.


## Business Value Delivered
- **Real-time Visibility**: Enabled branch managers to monitor revenue trends with < 1-minute latency.
- **Operational Insights**: Identified peak sales hours to optimize staffing schedules.
- **Data Reliability**: Guaranteed "Exactly-Once" processing semantics through robust checkpointing and Delta Lake transactions.
