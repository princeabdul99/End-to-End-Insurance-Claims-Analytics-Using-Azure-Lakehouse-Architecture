# End-to-End Insurance Claims Analytics Using Azure Lakehouse Architecture

## 📚  Project Overview

This project demonstrates an end-to-end insurance claims analytics solution built with Azure-native cloud services.
It showcases the full pipeline — from raw data ingestion to business intelligence reporting — using modern data lakehouse architecture principles.

### Objectives
- Improve claims processing efficiency.
- Enhance reserve estimation accuracy.
- Monitor payment behaviors and broker performance.
- Detect potential claim anomalies or fraud.
- Analyze regional and product-based claim trends.

## 🛤️ Solution Architecture

![image_alt](https://github.com/princeabdul99/End-to-End-Insurance-Claims-Analytics-Using-Azure-Lakehouse-Architecture/blob/a0e90e01024ada47a536e63964cc46536b325cdd/docs/architecture.drawio.png)

**1. Raw Data Ingestion (Bronze Layer)**
- Insurance datasets (claims announcements, payments, reserves, policies, brokers, products, coverages) ingested into Azure Data Lake Storage (raw zone).
- Data movement orchestrated using Azure Data Factory (ADF) pipelines.

**2. Data Cleaning and Transformation (Silver Layer)**
- **Azure Databricks** used to perform data cleansing, validation, and transformation.
- Business rules applied to correct negative payments, standardize dates, and resolve data quality issues.

**3. Curated Data Modeling (Gold Layer)**
- Transformed data modeled into **star schema structures** (fact and dimension tables) using **Azure Synapse Analytics**.
- Designed for efficient querying and optimized for reporting purposes.
  
**4. Power BI Connectivity Setup**
- Established **direct live connections** from **Azure Synapse** to **Power BI** for real-time data access.
- No dashboard or report creation included — future-ready for business intelligence development.

**5. Documentation and Project Management**
- Architectural diagrams created using draw.io.
- Project documentation, planning, and assumptions recorded in Notion.
- Source control and versioning managed through GitHub.

## 🛠️ Tech Stack

| Technology                  | Purpose                                                                           |
|-----------------------------|-----------------------------------------------------------------------------------|
| Azure Data Factory (ADF)    | Orchestrate ETL workflows and data movement.                                       |
| Azure Databricks            | Perform data cleaning, transformation, and validation.                             |
| Azure Synapse Analytics     | Curated SQL data models and optimized query layer.                                 |
| Azure Data Lake Storage     | Store raw, cleansed, and curated data.                                             |
| Power BI                    | Build business dashboards and KPIs.                                                |
| draw.io                     | Create system architecture and pipeline diagrams.                                  |
| Notion                      | Document project planning, assumptions, and lessons learned.                       |
| GitHub                      | Version control for ETL scripts, notebooks, and assets                             |

---

## 🗂️ Data Sources
The project processes multiple insurance-related datasets:

| Dataset               | Description                                                                                   |
|-----------------------|-----------------------------------------------------------------------------------------------|
| Claim Announcements   | Initial reporting of insurance claims.                                                   |
| Claim Payments        | Payments made toward claim settlements.                                                  |
| Claim Reserves        | Reserves allocated for pending claims.                                                   |
| Policies              | Policyholder and contract details.                                                       |
| Brokers               | Insurance brokers associated with policies.                                              |
| Products              | Insurance products offered.                                                              |
| Coverages             | Types of coverage included in policies.                                                  |
| Regions / States      | Geographical classification for claims analysis                                          |

---



## 📂 Repository Structure
```
Design-and-Deployment-of-a-Fashion-Retail-Data-Warehouse/
│
├── datasets/                           # Raw datasets used for the project (Csv data)
│
├── docs/                               # Project documentation and architecture details
│   ├── data_architecture.png           # Shows the project's architecture
│   ├── data_catalog.md                 # Catalog of datasets, including field descriptions and metadata
│   ├── data_flow.drawio                # Draw.io file for the data flow diagram
│   ├── data_models.drawio              # Draw.io file for data models (star schema)
│   ├── naming-conventions.md           # Consistent naming guidelines for tables, columns, and files
│
├── scripts/                            # SQL scripts for ETL and transformations
│   ├── bronze/                         # Scripts for extracting and loading raw data
│   ├── silver/                         # Scripts for cleaning and transforming data
│   ├── gold/                           # Scripts for creating analytical models
│
├── tests/                              # Test scripts and quality files
│
├── README.md                           # Project overview and instructions

```
---	
	
	
	
	
	
	

	
