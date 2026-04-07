# databricks-lakehouse-ecommerce-incremental-pipeline
End-to-end E-commerce Data Pipeline built on Databricks using Lakehouse Federation, Watermark logic, and Medallion Architecture. Features incremental processing (SCD Type 2), Delta Lake MERGE operations, and automated orchestration via Databricks Workflows.

# Incremental E-commerce Data Pipeline: Lakehouse Federation & Medallion Architecture

## 📌 Overview

I have created this project from scratch & published it on my YouTube Channel : http://www.youtube.com/@DataToCrunch .

<img width="1650" height="919" alt="image" src="https://github.com/user-attachments/assets/facae52d-e2d0-4089-a6e6-a4661a2560f8" />


This project demonstrates a production-grade, end-to-end data pipeline built on the **Databricks Lakehouse** platform. Using a practical e-commerce case study (Products, Orders, and Payments), the pipeline is designed to process data incrementally, avoiding full reloads and ensuring scalability.

The project covers the transition from traditional ETL to a modern **Lakehouse Federation** approach, focusing on idempotency, watermark logic, and state management.

<img width="1532" height="924" alt="image" src="https://github.com/user-attachments/assets/63bb042e-49ea-4745-bb70-52b4701481c3" />


---

## 🏗️ Architecture

Azure SQL DB → Lakehouse Federation (Unity Catalog) → Databiricks Repos → GitHub repo → Bronze (Incremental) → Silver (Merge/Upsert) → Gold (SCD Type 2) → BI Dashboards

<img width="1378" height="848" alt="image" src="https://github.com/user-attachments/assets/a78da4ca-5d6b-46c5-8286-7423de988127" />


---

## ⚙️ Tech Stack

- **Databricks Lakehouse Federation:** Direct source connectivity via Unity Catalog.
- **Delta Lake:** ACID transactions and `MERGE` (Upsert) capabilities.
- **Python (PySpark):** Core transformation and watermark logic.
- **Databricks Workflows:** Sequence orchestration and alerting.
- **GitHub + Databricks Repos:** Version control and CI/CD integration.
- **Unity Catalog:** Governance and connection management.

---

## 🔄 Pipeline Design

### 🔹 Version Control & GitHub Integration
- **Databricks Repos:** The entire pipeline code is managed via **Databricks Repos**, providing a direct sync with **GitHub**.
- **Collaboration:** Enables professional software development lifecycle (SDLC) practices, including branch management, code reviews, and seamless promotion between Dev, QA, and Production environments.

### 🔹 Ingestion (Lakehouse Federation)
Instead of traditional connectors, this pipeline uses **Lakehouse Federation** to establish a direct connection between Azure SQL Database and Unity Catalog. This allows the pipeline to query source tables as if they were local Delta tables, simplifying the ingestion footprint.

### 🥉 Bronze Layer (Raw & Incremental)
- **Watermark Logic:** Uses a **Control Table** to store the last processed `Timestamp` and `Primary Key`.
- **Incremental Ingestion:** Only pulls records created or updated since the last run.
- **Metadata:** Adds ingestion timestamps and source file paths to the raw Delta tables.

### 🥈 Silver Layer (Clean & Standardized)
- **Data Quality:** Performs deduplication and handles quarantined records.
- **Delta MERGE:** Uses the `MERGE` operation to perform upserts, ensuring the Silver layer reflects the most recent state of each entity without duplicates.

### 🥇 Gold Layer (Business & History)
- **Join Logic:** Interconnects Products, Orders, and Payments to create a unified business view.
- **SCD Type 2:** Maintains full historical changes for analytical reporting, allowing users to track how entities (like product prices or order statuses) evolved over time.

## ⚙️ Jobs, Workflows & Monitoring

### **Orchestration with Databricks Workflows**
The pipeline is fully automated using **Databricks Workflows**, executing the notebooks directly from the integrated **GitHub Repo**:
- **Task Chaining:** Bronze → Silver → Gold tasks are executed in sequence with automated retries.
- **Conditional Triggers:** The workflow is designed to be **Idempotent**, ensuring it can be safely re-run without duplicating data.

### **BI Reporting (SQL Dashboards)**
- **Databricks SQL:** The Gold layer powers a native **SQL Dashboard** within Databricks.
- **Real-Time Insights:** Provides stakeholders with visual tracking of order volumes, payment success rates, and product trends directly from the Lakehouse.
- **Automatic Refresh:** The dashboard is configured to refresh automatically as part of the final step in the Workflow.

### **Legacy Alerts & Observability**
- **Job Failure Alerts:** Configured to notify the engineering team via email or Slack if any task in the sequence fails.
- **Data Quality Alerts:** Utilizes **Legacy Alerts** on SQL queries to monitor the Gold layer for anomalies (e.g., if "Total Payments" drops below a specific threshold), ensuring operational reliability.

---

## 🚀 Key Concepts & Features

- **Incremental loading** (No full reloads)
- **Watermark logic** (Timestamp + Primary Key)
- **Delta Lake MERGE** (Upsert)
- **GitHub Integration** via Databricks Repos
- **Monitoring** via SQL Dashboards & Alerts

---

## ⚙️ Orchestration & Monitoring

The pipeline is fully automated using **Databricks Workflows**, ensuring a reliable and hands-off production environment:

* **Task Chaining:** Orchestrates the Medallion Architecture by executing Bronze → Silver → Gold tasks in strict sequence.
* **Dashboard Refresh:** Automatically triggers a refresh of BI visualizations upon the successful completion of the Gold layer task.
* **Alerting & Observability:** Configured email and Slack notifications to alert the team on pipeline failures, task retries, or specific data quality breaches.

<img width="2879" height="1111" alt="image" src="https://github.com/user-attachments/assets/944076bf-89dc-4415-a0f9-994985173a4a" />

<img width="2656" height="1306" alt="image" src="https://github.com/user-attachments/assets/feecbefb-0441-42e6-aa3b-a73573ec33b6" />

<img width="1642" height="1322" alt="image" src="https://github.com/user-attachments/assets/a0cd04df-5aae-453a-abfe-d2446b5ebd81" />

<img width="2341" height="1051" alt="image" src="https://github.com/user-attachments/assets/e0c46d2b-af94-412c-9c0d-646bcd80d576" />

---

## ▶️ How to Run

1.  **Unity Catalog Setup:** Configure a **Lakehouse Federation** connection in Unity Catalog to your Azure SQL Database source.
2.  **Clone Repository:** Use **Databricks Repos** to clone this GitHub repository directly into your workspace.
3.  **Initialize Control Table:** Execute the setup script provided in the `config/` folder to create the metadata table for tracking watermarks.
4.  **Databricks SQL Dashboards:** Powered by the Gold Layer for real-time insights.
5.  **Configure Workflow:** Create a new **Databricks Job**, adding the notebooks from the `notebooks/` folder as sequential tasks.
6.  **Run & Monitor:** Manually trigger the job or set a cron schedule. Monitor the incremental data flow and state changes through the **Job Run UI**.

---

## 🎥 YouTube Walkthrough

https://youtu.be/by-pDbTrnsw?si=rUQT7Xfv5JIE3cO0

https://youtu.be/LwX_M070bNI?si=qHgyA7X59ZEVSnGy

---

## 📊 BI Dashboard Insights

The Gold layer acts as the single source of truth, powering interactive dashboards that provide:

* **Real-time Order Tracking:** Monitoring order volumes and statuses as they are processed incrementally.
* **Product & Payment Analysis:** Visualizing product performance and payment distribution across different categories.
* **Historical Trends:** Utilizing **SCD Type 2** data to perform "point-in-time" analysis of changing entity attributes.
  
<img width="2656" height="1306" alt="image" src="https://github.com/user-attachments/assets/feecbefb-0441-42e6-aa3b-a73573ec33b6" />

---

## 📌 Dataset

**E-commerce Case Study:** This project utilizes a custom-generated dataset consisting of `Products`, `Orders`, and `Payments`. The data is specifically designed to simulate high-frequency inserts and updates to demonstrate the robustness of the incremental watermark logic.
