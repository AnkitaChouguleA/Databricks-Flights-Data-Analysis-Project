# End-to-End Airline Analytics Pipeline (Databricks & Power BI)

## 📌 Overview
This project demonstrates the construction of a modern, scalable, end-to-end airline analytics pipeline. It uses **Databricks** (PySpark, Delta Lake, Delta Live Tables) to ingest, process, and model raw data from GitHub, following the **Medallion Architecture**. The final cleansed and modeled data is consumed by **Power BI** to deliver interactive business intelligence dashboards.

---

## 🔍 Problem Statement
Airlines generate massive volumes of operational data (bookings, routes, logs, revenue). The core business challenge is transforming this raw, siloed data into **actionable, near real-time insights** that decision-makers can use to optimize operations, enhance customer experience, and improve business strategy. This project provides an automated and robust data foundation to solve this crucial need.

---

## 📊 Tools & Technologies Used
- **Cloud Platform:** Microsoft Azure (or general cloud environment for Databricks)
- **Data Lakehouse:** **Databricks** (Unified Analytics Platform)
- **Data Storage:** **Delta Lake** (for reliable, high-performance storage)
- **ETL/ELT Framework:** **Delta Live Tables (DLT)** for declarative pipeline management
- **Ingestion:** **Databricks Autoloader** (for incremental file processing)
- **Programming:** PySpark, Python (for initial GitHub ingestion script)
- **Data Governance:** **Unity Catalog**
- **Visualization:** **Power BI**
- **Version Control:** Git & GitHub

---

## 📁 Dataset
The project utilizes a comprehensive airline dataset sourced from a GitHub repository, spanning four core operational domains essential for full-spectrum analysis:
- **Flights Data:** Includes flight ID, origin, destination, and airline information.
- **Passengers Data:** Contains passenger demographics (ID, name, nationality, gender).
- **Airports Data:** Details airport codes, cities, and countries.
- **Bookings Data:** Transactional data linking passengers to flights, including booking dates and revenue amounts.

---

## 🧠 Approach
The solution is built around the **Medallion Architecture** (Bronze → Silver → Gold) to ensure data quality, structure, and governance:

1.  **Source/Raw Ingestion:** A Python script fetches raw CSV files from GitHub, decodes them, and stages them into **Unity Catalog Volumes** for centralized access.
2.  **Bronze Layer:** **Databricks Autoloader** incrementally ingests the raw files into Bronze layer Delta tables, providing schema evolution handling and reliability.
3.  **Silver Layer:** **Delta Live Tables (DLT)** pipelines perform automated cleansing, enrichment, and quality enforcement:
    * Standardizing data types and handling nulls.
    * Applying data quality expectations using DLT features (e.g., `@dlt.expect_all_or_drop`).
4.  **Gold Layer:** The clean Silver data is transformed into a **Star Schema** (Dimensional Model) optimized for analytical querying:
    * Created Dimension tables (`DimPassengers`, `DimFlights`, `DimAirports`).
    * Created a Fact table (`FactBookings`) with key metrics and joins.
    * **Delta Merge** logic is used for efficient Change Data Capture (CDC) and upsert operations.
5.  **Consumption:** The Gold layer tables are exposed via Databricks SQL Endpoints and consumed by **Power BI** for interactive dashboards.

---

## 📈 Key Features
- **Scalable Data Ingestion:** Uses Autoloader for robust, incremental, and cloud-native file ingestion.
- **Automated Data Quality:** Implements **Delta Live Tables** (DLT) for declarative pipeline definition and automatic quality checks.
- **Dimensional Modeling:** Adopts a **Star Schema** to enable easy and high-performance querying by analysts and BI tools.
- **Interactive Power BI Dashboard:** Provides key insights across three areas:
    * **Executive Summary:** Key KPIs for total revenue, airline count, and global reach.
    * **Passenger Analysis:** Breakdown of demographics, spend by nationality, and loyalty insights.
    * **Flights & Routes:** Detailed analysis of route-level bookings, revenue, and airport traffic patterns.

---

## ✅ Results
- **Operational Efficiency:** Reduced manual data reporting and preparation efforts by an estimated **80%** through pipeline automation.
- **Improved Data Integrity:** Achieved high data quality and consistency enforced by DLT and the structured Medallion Architecture.
- **Faster Decision Making:** Enabled leadership and analysts to make faster, data-backed decisions using near real-time updates from the pipeline.

---

## 📷 Screenshots
<img width="1100" height="694" alt="image" src="https://github.com/user-attachments/assets/d875aadf-65f1-4e68-ac97-1d7ca4b5db31" />


<img width="1100" height="494" alt="image" src="https://github.com/user-attachments/assets/50a0b58d-1128-494d-b455-d6cae1cf5926" />


<img width="1100" height="508" alt="image" src="https://github.com/user-attachments/assets/0b84554e-dd4f-4398-b367-7c44e4d87042" />


<img width="1100" height="618" alt="image" src="https://github.com/user-attachments/assets/7fce572d-05c2-44a8-9133-58f0c4eb0994" />


<img width="1100" height="631" alt="image" src="https://github.com/user-attachments/assets/5532ef04-c809-41ba-a07f-4ae4b9d2c52e" />


<img width="1100" height="638" alt="image" src="https://github.com/user-attachments/assets/ac6a288b-773f-48a7-9cab-0058667c69de" />


---

## 📚 Learnings
- Gained hands-on experience in building a complete ETL/ELT pipeline using Databricks' modern components.
- Mastered the use of **Databricks Autoloader** for simplified, schema-evolving data ingestion.
- Learned to use **Delta Live Tables** (DLT) to define data transformations and quality checks declaratively.
- Successfully implemented the **Medallion Architecture** to structure a modern Lakehouse environment.
- Effectively connected Databricks SQL Endpoints to **Power BI** to bridge big data processing with business intelligence reporting.

---

## 🤝 Acknowledgements
**Medium Post:** [️ Building an End-to-End Airline Analytics Pipeline with Databricks & Power BI](https://medium.com/@ankitanavalgunde161999/%EF%B8%8F-building-an-end-to-end-airline-analytics-pipeline-with-databricks-power-bi-40d26ebddc2a)
**Data Source:** Project's raw data files are hosted on GitHub.
