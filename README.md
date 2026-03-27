# Social Media Engagement Pipeline 🚀

This project implements a robust **ETL (Extract, Transform, Load)** pipeline using **PySpark** to analyze social media performance. It processes raw data about users, posts, and engagements to provide actionable business insights.

## 📊 Data Architecture: The Medallion Approach
We follow the **"Medallion Architecture"** to ensure data quality at every step:

* **Bronze Layer (Raw):** Original, messy CSV files (`users.csv`, `posts.csv`, `engagement.csv`) with inconsistent formatting and missing values.
* **Silver Layer (Cleaned):** Data is standardized (IDs formatted, countries capitalized, null values handled, and duplicates removed).
* **Gold Layer (Analytics):** Business-level tables used for final reporting and high-level KPIs.

---

## 🛠️ Visual Workflow

```mermaid
graph TD
    A[Raw CSVs: Bronze] -->|Cleaning & ID Mapping| B[Standardized: Silver]
    B -->|Joins & Aggregation| C[Insights: Gold]
    
    subgraph "Cleaning Steps"
    B1[Remove Duplicates]
    B2[Fix N/A Values]
    B3[ID Range Alignment]
    end
    
    subgraph "Business Goals"
    C1[Top Creators]
    C2[Category Performance]
    C3[Regional Activity]
    C4[Underperforming Content]
    end
````

# 🔍 The ID Discrepancy Challenge (Crucial Insight)
During the development, we identified a critical data integrity issue:

The Problem: The posts.csv contains IDs in the range P100001 to P100224, but the engagement.csv references IDs from P101000 upwards.

The Consequence: A standard SQL Join would return zero results, as the "Crowd" (Engagements) is looking for "Seats" (Posts) that don't exist in the current export.

Our Solution (Modulo Mapping): To make the pipeline functional for testing and demonstration, we implemented a Mathematical Mapping Logic. We use the MOD operator to wrap the high engagement IDs back into the valid post range (1-224).

Result: This ensures that every engagement record is assigned to a real post. While the specific assignment is "synthetic" for this test, it allows us to validate the entire logic chain (Aggregation, Top Performers, Regional Metrics) which will work perfectly once the source systems provide matching ID ranges.

# 🎯 Business Goals Addressed
The pipeline is designed to answer five critical business questions:

Highest Engagement Creators: Who are our most impactful influencers?

Best Content Categories: Which topics (Health, Tech, Travel) resonate most with the audience?

Active Regions: Which geographical markets show the highest interaction volume?

Underperforming Posts: Identifying "Ghost Posts" with zero engagement to optimize content strategy.

Data Readiness: Creating a clean, joined dataset for future BI tools (PowerBI/Tableau).

# 🔧 Technical Key Features
Smart Data Cleaning: Automatically handles "N/A" strings in numeric columns and standardizes inconsistent casing (e.g., "usa" vs "USA").

ID Mapping Logic: Includes a mathematical "Modulo-Map" to align engagement data with available post IDs, ensuring 100% join reliability for testing.

Memory Efficiency: Utilizes Spark's .cache() for optimized multi-report generation.

Error Handling: Implements try-cast and coalesce to prevent pipeline crashes due to corrupted input data.

# 🚀 How to Run
Preparation: Place your raw files in /data/bronze/.

Execution: Run the pipeline using the uv package manager:

uv run main.py

Results:

The console will print the Top 5 lists for each Business Goal.

The full consolidated Gold dataset is saved in /data/gold/ as a CSV for further analysis.