# 🚀 Containerized Live Financial Market Data Analysis & Alerting System

### Overview
This project delivers a **containerized, reliable, and configuration-driven** Data Engineering pipeline using **Apache Airflow** to extract, transform, and load live financial market data (ETL/ELT). The data is housed in a PostgreSQL Star Schema, culminating in a dynamic Power BI dashboard for real-time analysis and operational monitoring.

**Key Technologies:** Apache Airflow, Docker, Docker Compose, PostgreSQL, Python (Pandas, yfinance), Power BI.

***

## 1. Architecture and Data Flow

The system follows a standard ELT (Extract, Load, Transform) pattern, where data is sourced, moved to the PostgreSQL warehouse, and then transformed using analytical SQL queries (DWH layer).

### Data Flow Diagram

A simple box diagram illustrating the data flow:

YFINANCE API (Source) --> AIRFLOW DAG (Orchestration & ETL) --> POSTGRESQL DB (Star Schema DWH) --> POWER BI (Dashboard & Analysis)

| Component | Role | Data Store / Tool |
| :--- | :--- | :--- |
| **Source** | Real-Time Data Ingestion | Yahoo Finance API (`yfinance`) |
| **Orchestration** | Workflow Automation, Scheduling, and Dependency Management | **Apache Airflow** (Docker) |
| **Data Warehouse** | Persistent Star Schema (Fact/Dim Tables) and Analytical Views | **PostgreSQL** (Docker) |
| **Visualization** | Business Intelligence & Operational Monitoring | Power BI Desktop |

***

## 2. Technical Achievements (Core Skills Demonstrated)

| Feature | Implementation | Data Engineering Skill Highlight |
| :--- | :--- | :--- |
| **Reliability** | **Exponential Backoff Retries** implemented during API calls (in the Python ETL code). | Ensures pipeline stability and fault tolerance against transient network/API failures. |
| **Automation** | Scheduled via Airflow using a precise **CRON scheduling**: `'0 9 * * 1-5'`. | Optimizes resource usage by running only on weekdays at market open (9:00 AM UTC). |
| **Data Integrity** | **UPSERT Logic** (`ON CONFLICT DO UPDATE`) used for the `fact_market_data` table. | Prevents duplicate records and maintains data accuracy upon pipeline re-runs or late data arrivals. |
| **Configurability** | Stock Tickers (`MSFT, AAPL, etc.`) and Alert Thresholds are loaded dynamically from **`config.json`**. | Decouples business rules from code logic for easy maintenance and scalability. |
| **Advanced Analysis** | Utilized **Window Functions** (`ROW_NUMBER() OVER (PARTITION BY Ticker)`) in SQL for calculating **Total Growth** across all stocks. | Demonstrates strong analytical SQL capability (e.g., in the `All_Tickers_Total_Growth` view). |
| **Alerting** | Implemented a check that triggers an email alert if any stock's daily percentage change exceeds the configured `alert_threshold`. | Provides immediate, automated response to significant market movements. |

***

## 3. Deployment and Setup

### Prerequisites
* Docker and Docker Compose
* Power BI Desktop

### Setup Steps
1.  **Clone the Repository:** Navigate to the project root directory.
2.  **Verify Configuration:** Ensure the `config.json` file is present in the `dags/` folder and contains the correct ticker symbols (e.g., **AAPL**).
3.  **Build and Deploy Containers:** Run the following commands:
    ```bash
    docker compose down
    docker compose build  # Builds custom Airflow image
    docker compose up -d 
    ```
4.  **Trigger DAG:**
    * Access Airflow UI at `http://localhost:8080` (User/Pass).
    * Enable the **`live_market_data_and_alerting_pipeline_final`** DAG.
    * Manually trigger the DAG to execute the ETL process.
5.  **Connect BI Tool:** Connect Power BI to the PostgreSQL container using the following details:
    * **Server:** `localhost:5432`
    * **Database:** `airflow`
    * **Authentication:** Basic (username, password)

***

## 4. Analytical Insights (Dashboard Summary)

The Power BI dashboard provides three dynamic views for analysis, built entirely from the PostgreSQL Star Schema.

### 4.1. KPI Page
This page shows high-impact summary metrics, including the Max Daily Gain/Loss, Average Volume, and Max Price Volatility.

**![alt text](KPI.png)**

### 4.2. Trend Page
This page displays the historical price movements. The **X-Axis** is set to **Categorical** to ensure a clean line chart connecting only the actual daily data points.

**![alt text](Trends.png)**

### 4.3. Data Monitoring Page
This page confirms the system's operational status. The card displays the latest timestamp recorded in the final data table, serving as a proxy for the time of the last successful ETL completion.

**![alt text](Monitor.png)**