# Data Pipeline - Loyalty Program Engagement

## Overview
This project simulates a **data pipeline for a user loyalty and engagement program**, describing how data flows across multiple internal systems — from source databases to merged warehouse tables, and finally to downstream teams such as **Marketing, Customer Success, Product, and Analytics** shown in this example.

The pipeline integrates and processes user behavior, support, and activity data to support **retention, engagement, and growth initiatives**.


## Data Architecture
The overall data flow follows three layers:

## Entity Relationship Diagram (ERD)
The ERD below shows the data relationships across major entities in the pipeline:

![ERD](/Main/ERD.jpg)

### Source Data (DW-A, DW-B, DW-C)
Multiple departmental databases feed raw and semi-processed data such as:
* Feature adoption logs
* Support tickets
* Customer engagement and activity records
* Regional benchmarks and metadata

## Data Sources
| Data Source | Description |
|--------------------|-------------|
| `User_Profile` | Basic platform user demographics and account info |
| `User_Activity` | Historical user interactions (logins, purchases, feedback) |
| `Engagement_Score` | Composite score based on user behavior and frequency |
| `Feature_Adoption` | Tracks usage of new features and platform modules |
| `Retention_Action` | Logs interventions for low-engagement users |
| `Support_Calls` | Number of support calls in the last 30 days |
| `Support_Chats` | Chat-based support interactions |
| `Issue_Tickets` | Technical or operational issue reports |
| `Service_Requests` | Requests for new services or features |
| `Product_Support_Usage` | Help article and FAQ usage data |
| `Repeat_Engagements` | Identifies repeated interactions |
| `Region_Metadata` | Region-level classification of users |
| `Region_Thresholds` | Baseline engagement thresholds by region |


### Merged Data (Warehouse Layer)
* A centralized SQL process merges all sources into:
* **Master Table** — the unified, most current engagement dataset.
* **History Table** — a timestamped log of prior daily snapshots.

**This merging and transformation is handled through the included SQL script, highlighted in yellow in the ERD, which consolidates all sources and prepares the outputs for downstream teams:**

**Master_Table_SQL.sql**

### Consumers (Downstream Teams)
* Marketing → Receives “Highly Engaged” users for loyalty campaigns (via S3 unload).
* Customer Success → Receives “Low Engagement” users for outreach programs (via S3 unload).
* Product → Accesses feature adoption metrics by region.
* Analytics → Generates dashboards and performance insights.

### Monitoring
A process is set up in extracting the load from the master table into dashboard on a scheduled refresh. The dashboard includes multiple monitoring tabs designed to track data load performance, detect anomalies, and validate data integrity. This automated setup significantly reduces manual monitoring efforts and accelerates troubleshooting efficiency — improving daily operational visibility and saving 5–10x the time compared to manual checks.

## Tech Stack
- **Snowflake SQL** – Data transformation, merge, and unload steps.
- **Python & Pandas** – Redaction, documentation, and metadata automation.
- **S3 (AWS)** – External data delivery for Marketing and CS teams.
- **Tableau / BI Tools** – Monitoring and analytics dashboards.

## Closing Note
All SQL code and field names have been redacted from a real-world enterprise pipeline and renamed for public release.
This repository is for demonstration and portfolio purposes.

