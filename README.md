# Build-With-Databricks-Hands-On-Project-Challenge
Ad-Tech Intelligence: End-to-End Predictive Analytics & ML Pipeline
Project Overview

This project demonstrates a production-grade Ad-Tech Data Lakehouse built on Databricks. It transforms raw, noisy ad-server logs into actionable business insights and a machine learning model capable of predicting ad-click probability (CTR).
Key Highlights:

    Architecture: Medallion (Bronze → Silver → Gold) using Delta Lake.

    Governance: Unity Catalog for data lineage and security.

    AI Innovation: Class-imbalanced click prediction using Random Forest and MLflow.

    Business Impact: Real-time revenue estimation and advertiser performance dashboard.

1. Data Architecture & Modeling

The system processes 100k+ events across 6 core entities. We utilize a Snowflake Schema logic where the id_md5 (transaction hash) links the request life-cycle.
The Medallion Flow:

    Bronze (Raw): Ingestion of synthetic CSV logs (Requests, Impressions, Clicks, User Profiles, Campaigns, and App Metadata).

    Silver (Cleansed): * Standardized inconsistent data (e.g., gender casing, state codes).

        Handled missing values using COALESCE.

        Deduplicated entries to mitigate "Click Fraud."

        Joined events into a fact_ad_funnel.

    Gold (Curated): * Engineered features: hour_of_day, day_of_week, and is_affinity_match.

        Aggregated tables for high-performance SQL analytics.

2. Problem Definition & AI Framing

    Objective: Binary classification to predict is_click.

    The "Why": Traditional rule-based ad serving fails to account for the complex interaction between user demographics (age/gender), device context, and app genre.

    Success Criteria: Achieve an ROC-AUC > 0.75, ensuring the model can effectively rank users even with a 10:1 class imbalance.

3. Machine Learning & Technical Reasoning

We utilized the Random Forest Classifier due to its robustness with categorical data and ability to capture non-linear feature interactions.
Model Performance & Interpretation:

    Handling Imbalance: Applied class_weight='balanced' to prevent the model from ignoring the minority "Click" class.

    Metrics: * ROC-AUC: 0.778 (Strong ranking capability).

        Recall (Class 1): 1.00 (Captured all potential clicks).

    Experiment Tracking: All runs, parameters (n_estimators, max_depth), and metrics were logged via MLflow.

4. Business Impact & Insights

The Databricks SQL Dashboard provides real-time visibility into:

    Revenue by State: Identifying high-conversion geographies.

    Campaign Type Performance: Comparing CPC vs. high-value CPI (Installs).

    Personalization: Using the is_affinity_match feature to prove that contextually relevant ads drive higher engagement.

5. Setup & Reproducibility

    Data Generation: Run generate_data.py locally to create the initial CSV files.

    Ingestion: Upload CSVs to a Databricks Volume and run 01_Bronze_Ingestion.

    Pipeline: Execute 02_Silver_Cleansing and 03_Gold_Features.

    Training: Run 04_ML_Model_Training to register the model in MLflow.

    Analytics: Import the .dash file or run the SQL queries in the SQL_Analytics folder.

6. Technical Requirements

    Databricks Runtime: 13.x+ (Includes Spark 3.4+, MLflow, and Scikit-Learn).

    Governance: Unity Catalog (Enabled).

    Storage: Delta Lake (ACID enabled).