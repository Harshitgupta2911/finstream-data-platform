# FinStream Data Platform

An end-to-end Financial Data Engineering + AI platform built using AWS S3, Databricks, Apache Kafka, dbt, Apache Airflow, and Generative AI.

---

## Overview

FinStream is an end-to-end financial data platform designed to simulate a production-oriented fintech data pipeline.

The platform supports both batch and streaming data pipelines and follows the Medallion Architecture:

Batch / Streaming Sources
        |
        v
      Bronze
        |
        v
      Silver
        |
        v
       Gold
        |
        +-------------------+
        |                   |
        v                   v
 Natural Language       AI Incident
      to SQL             Summarizer
        |                   |
        v                   v
   Analytics            Incident
    Queries            Explanation

The project contains two separate AI layers:

1. Natural Language to SQL
2. AI Incident Summarizer

---

## Architecture

                         FINSTREAM DATA PLATFORM

       BATCH DATA                                      STREAMING DATA
           |                                                  |
           v                                                  v
   Synthetic Data                                      Transaction
     Generator                                           Producer
           |                                                  |
           v                                                  v
        AWS S3                                          Apache Kafka
           |                                      finstream.transactions
           |                                                  |
           |                                                  v
           |                                           Kafka Consumer
           |                                                  |
           |                                                  v
           +------------------------------------------> AWS S3
                                                          |
                                                          v
                                                    Databricks
                                                          |
                                                          v
                                                     BRONZE
                                                          |
                                                          v
                                                     SILVER
                                                          |
                                                          v
                                                       GOLD
                                                          |
                                  +-----------------------+----------------------+
                                  |                                              |
                                  v                                              v
                           AI LAYER 1                                    AI LAYER 2
                      Natural Language to SQL                         AI Incident Summarizer
                                  |                                              |
                                  v                                              v
                            SQL Queries                                  Incident Summary
                                  |
                                  v
                           Analytics Results

                              Apache Airflow
                             Orchestration Layer

---

## Technology Stack

| Category | Technology |
|---|---|
| Programming | Python |
| Cloud Storage | AWS S3 |
| Data Processing | Databricks / Apache Spark |
| Data Warehouse / Lakehouse | Databricks |
| Streaming | Apache Kafka |
| Containerization | Docker / Docker Compose |
| Transformation | dbt |
| Orchestration | Apache Airflow |
| Data Format | Parquet |
| Synthetic Data | Faker |
| Data Quality | Python / SQL |
| Anomaly Detection | Isolation Forest |
| Generative AI | Google Gemini |
| Version Control | Git / GitHub |
| Configuration | python-dotenv |

---

## Data Sources

FinStream uses synthetic financial data generated using Faker.

The platform contains the following datasets:

### Customers

Customer master information.

Important fields include:

- customer_id
- first_name
- last_name
- email
- phone
- date_of_birth
- country
- created_at
- customer_status

### Accounts

Financial account information associated with customers.

### Merchants

Merchant information associated with transactions.

### Transactions

Financial transaction records.

Important fields include:

- transaction_id
- account_id
- merchant_id
- transaction_timestamp
- transaction_type
- amount
- currency
- transaction_status
- payment_method
- description

### Transaction Events

Events associated with the lifecycle of transactions.

Examples include:

- initiated
- authorized
- completed
- reversed
- failed

### Exchange Rates

Currency exchange-rate information used for financial transformations and analysis.

---

# Medallion Architecture

FinStream follows the Bronze → Silver → Gold architecture.

## Bronze Layer

The Bronze layer stores raw or minimally transformed data.

Data is ingested from AWS S3 and streaming sources into Databricks.

Bronze contains:

- Customers
- Accounts
- Merchants
- Transactions
- Transaction Events
- Exchange Rates
- Streaming Transactions

Metadata such as ingestion timestamp and source file information is also maintained.

Purpose:

- Preserve source data
- Maintain raw history
- Provide a reliable ingestion layer
- Support reprocessing

---

## Silver Layer

The Silver layer contains cleaned and validated data.

Processing includes:

- Data type standardization
- Duplicate removal
- Null handling
- Data validation
- Business-rule validation
- Relationship validation
- Transaction attribute standardization

Purpose:

- Create clean datasets
- Enforce data quality
- Prepare data for analytical modeling

---

## Gold Layer

The Gold layer contains business-ready analytical models.

Example models include:

- dim_customer
- dim_merchant
- fact_transaction

The Gold layer is consumed by:

- Analytics
- Reporting
- Natural Language to SQL
- Anomaly Detection
- AI Incident Summarizer

---

# Batch Pipeline

The batch pipeline generates synthetic financial datasets and stores them in AWS S3.

Pipeline:

    Faker
      |
      v
    Python
      |
      v
  Parquet Files
      |
      v
     AWS S3
      |
      v
  Databricks
      |
      v
    Bronze
      |
      v
    Silver
      |
      v
     dbt
      |
      v
     Gold

AWS S3 acts as the raw data lake storage layer.

---

# Streaming Pipeline

FinStream also supports transaction streaming using Apache Kafka.

Pipeline:

    Transaction Producer
            |
            v
       Apache Kafka
            |
            v
    finstream.transactions
            |
            v
       Kafka Consumer
            |
            v
           AWS S3
            |
            v
        Databricks
            |
            v
         Bronze
            |
            v
         Silver
            |
            v
          Gold

Kafka topic:

    finstream.transactions

Streaming S3 location:

    s3://finstream-data-ingestion/raw/stream_transactions/

The streaming pipeline allows real-time transaction events to become part of the same analytical data platform as the batch data.

---

# dbt Transformation Layer

dbt is used to build and manage analytical transformations.

Responsibilities include:

- SQL transformations
- Gold-layer modeling
- Jinja templating
- Macros
- Data tests
- Incremental models
- Model dependencies

Example Gold models:

    gold.dim_customer
    gold.dim_merchant
    gold.fact_transaction

The Gold layer provides trusted datasets for analytics and AI.

---

# Data Quality

Data quality checks are performed before downstream AI and analytics processing.

The platform checks for issues such as:

- Missing values
- Duplicate records
- Invalid relationships
- Invalid transaction states
- Unexpected transaction patterns
- Invalid data types
- Business-rule violations

The objective is to prevent unreliable data from reaching downstream consumers.

---

# Anomaly Detection

FinStream includes machine-learning-based anomaly detection.

Isolation Forest is used to identify unusual transaction behavior.

Pipeline:

    Gold Data
       |
       v
    Feature Preparation
       |
       v
    Anomaly Detection
       |
       v
    Anomaly Report
       |
       v
    AI Incident Summarizer

The anomaly report becomes an input to the second AI layer.

---

# AI Architecture

FinStream contains two independent AI layers.

                    GOLD DATA
                       |
             +---------+---------+
             |                   |
             v                   v
       AI LAYER 1           AI LAYER 2
       NL → SQL             Incident AI
             |                   |
             v                   v
        SQL Query          Incident Summary
             |                   |
             v                   v
        Data Result        AI Incident Table

---

# AI Layer 1 — Natural Language to SQL

The first AI layer allows users to interact with financial data using natural language.

Instead of manually writing SQL, a user can ask a question in plain English.

Example:

    What is the total transaction amount for each transaction type?

The AI generates SQL similar to:

    SELECT
        transaction_type,
        SUM(amount) AS total_amount
    FROM gold.fact_transaction
    GROUP BY transaction_type;

The query is then executed against the Gold layer.

Flow:

    User Question
          |
          v
        Gemini
          |
          v
      SQL Generation
          |
          v
      SQL Validation
          |
          v
       Gold Layer
          |
          v
      Query Result

Purpose:

- Natural Language to SQL
- Self-service analytics
- LLM-powered data access
- Schema-aware query generation
- Business-user friendly analytics

---

# AI Layer 2 — AI Incident Summarizer

The second AI layer focuses on data incidents and anomalies.

It receives anomaly information from the anomaly-detection pipeline and uses Google Gemini to generate an understandable incident explanation.

Flow:

    Gold Data
       |
       v
    Data Quality
       |
       v
    Anomaly Detection
       |
       v
    Anomaly Report
       |
       v
    AI Incident Summarizer
       |
       v
    Incident Summary
       |
       v
    AI Incident Table

The AI Incident Summarizer can explain:

- What happened
- Why the record was considered anomalous
- Which records were affected
- Important transaction information
- Possible business impact
- Suggested investigation direction

This turns raw anomaly output into a human-readable incident report.

---

# AI Incident Table

The AI incident summaries are designed to be stored separately in an AI layer.

Conceptually:

    Gold
      |
      v
    Anomaly Detection
      |
      v
    AI Incident Summarizer
      |
      v
    AI Layer
      |
      v
    AI Incident Summary Table

This keeps AI-generated information separate from the core Bronze, Silver, and Gold analytical data.

---

# Apache Airflow Orchestration

Apache Airflow is used to orchestrate the FinStream platform.

The orchestration layer coordinates the execution of the data-processing and AI workflows.

Example workflow:

    Ingestion
       |
       v
    Bronze Processing
       |
       v
    Silver Processing
       |
       v
    Gold Transformation
       |
       v
    Data Quality
       |
       v
    Anomaly Detection
       |
       v
    AI Incident Summarization

Airflow triggers the required Databricks jobs and coordinates the overall pipeline execution.

---

# Databricks

Databricks is the primary data-processing platform.

It is used for:

- Reading data from AWS S3
- Bronze ingestion
- Silver transformations
- Gold processing
- Spark-based processing
- Streaming processing
- Analytical queries
- AI data preparation

Databricks notebooks are maintained as part of the project repository so that the processing logic is version-controlled along with the rest of the codebase.

---

# Project Structure

    finstream-data-platform/
    |
    +-- src/
    |   +-- finstream_data_platform/
    |       +-- ingestion/
    |       +-- transformation/
    |       +-- streaming/
    |       +-- quality/
    |       +-- ai/
    |
    +-- notebooks/
    |   +-- bronze/
    |   +-- silver/
    |   +-- gold/
    |   +-- streaming/
    |   +-- ai/
    |
    +-- dbt/
    |   +-- models/
    |   +-- macros/
    |   +-- tests/
    |   +-- dbt_project.yml
    |
    +-- dags/
    |   +-- finstream_stream_orchestration.py
    |
    +-- Kafka/
    |   +-- docker-compose.yml
    |
    +-- Data/
    |   +-- raw/
    |
    +-- tests/
    |
    +-- .env.example
    +-- .gitignore
    +-- pyproject.toml
    +-- README.md

---

# Security

Secrets and credentials are not committed to GitHub.

Sensitive values are provided using environment variables.

Example:

    AWS_ACCESS_KEY_ID=
    AWS_SECRET_ACCESS_KEY=
    S3_BUCKET_NAME=
    DATABRICKS_HOST=
    DATABRICKS_TOKEN=
    GEMINI_API_KEY=
    GEMINI_MODEL=

The actual .env file should never be committed.

Only .env.example should be included in the repository.

---

# Installation

## Clone the repository

    git clone <your-github-repository-url>

    cd finstream-data-platform

## Create environment

Install the project dependencies using the project's configured Python environment.

Configure the required environment variables using:

    .env.example

---

# Running Kafka

Navigate to the Kafka directory:

    cd Kafka

Start Kafka:

    docker compose up -d

Check Kafka:

    docker compose ps

List topics:

    docker exec finstream-kafka /opt/kafka/bin/kafka-topics.sh \
        --list \
        --bootstrap-server localhost:9092

Expected topic:

    finstream.transactions

---

# Running the Data Pipeline

The overall pipeline follows:

    Data Generation
          |
          v
       AWS S3
          |
          v
       Bronze
          |
          v
       Silver
          |
          v
        dbt
          |
          v
        Gold
          |
          v
    Data Quality
          |
          v
    Anomaly Detection
          |
          v
    AI Incident Summarizer

Streaming transactions follow:

    Producer
       |
       v
     Kafka
       |
       v
    Kafka → S3
       |
       v
    Databricks
       |
       v
    Bronze → Silver → Gold

---

# Example AI Questions

The Natural Language to SQL layer can support questions such as:

    What is the total transaction amount?

    What are the most common transaction types?

    Which merchants have the highest transaction volume?

    Which customers have the highest transaction value?

    How many transactions failed?

    What is the average transaction amount by currency?

    What are the daily transaction volumes?

The exact questions supported depend on the available Gold-layer schema and SQL-generation implementation.

---

# Example Incident Analysis

The anomaly-detection system may identify an unusual transaction pattern.

Instead of exposing only a technical anomaly record:

    transaction_id = XYZ
    anomaly_score = -0.82
    anomaly = True

the AI Incident Summarizer converts the information into a business-readable explanation.

Example structure:

    Incident:
    Unusual transaction behavior detected.

    Summary:
    The transaction deviates from the expected transaction pattern
    based on the available transaction features.

    Possible Impact:
    The transaction may require further investigation.

    Recommended Action:
    Review the transaction and related account activity.

The actual summary is generated dynamically by the AI model.

---

# GitHub

The project uses Git for version control.

The repository contains:

- Python source code
- Databricks notebooks
- dbt models
- Airflow DAGs
- Kafka configuration
- AI components
- Project documentation

Large generated datasets, credentials, local environments, and other unnecessary files should be excluded using .gitignore.

---

# Project Objectives

This project demonstrates practical experience with:

- End-to-end data engineering
- Data lake architecture
- Medallion architecture
- Batch data pipelines
- Streaming data pipelines
- Apache Kafka
- AWS S3
- Databricks
- Apache Spark
- dbt
- Apache Airflow
- Data quality engineering
- Machine-learning-based anomaly detection
- Generative AI
- Natural Language to SQL
- AI-assisted incident analysis
- Cloud data platforms
- Git and GitHub

---

# Future Improvements

Potential improvements include:

- Real-time anomaly detection directly from Kafka
- Real-time AI incident summarization
- Advanced data-quality monitoring
- Data lineage
- Data observability dashboards
- CI/CD for Databricks and dbt
- Automated pipeline testing
- SQL safety validation for Natural Language to SQL
- Role-based access control
- RAG-based business knowledge
- Pipeline monitoring and alerting
- Infrastructure as Code
- Cloud cost optimization
- Production-grade deployment

---

# Final Architecture

    +-----------------------------------------------------------+
    |                    DATA SOURCES                           |
    |                                                           |
    |  Synthetic Batch Data              Streaming Transactions |
    +-------------------+-------------------------+-------------+
                        |                         |
                        v                         v
                    AWS S3                    Apache Kafka
                        |                         |
                        |                         v
                        |                    Kafka → S3
                        |                         |
                        +------------+------------+
                                     |
                                     v
                           +-------------------+
                           |    DATABRICKS     |
                           +---------+---------+
                                     |
                                     v
                           +-------------------+
                           |      BRONZE       |
                           |   Raw Ingestion   |
                           +---------+---------+
                                     |
                                     v
                           +-------------------+
                           |      SILVER       |
                           | Clean & Validate  |
                           +---------+---------+
                                     |
                                     v
                           +-------------------+
                           |       GOLD        |
                           | Business Models   |
                           +---------+---------+
                                     |
                   +-----------------+-----------------+
                   |                                   |
                   v                                   v
          +-------------------+               +-------------------+
          |    AI LAYER 1     |               |    AI LAYER 2     |
          |                   |               |                   |
          | Natural Language  |               | AI Incident       |
          |      → SQL        |               | Summarizer         |
          +---------+---------+               +---------+---------+
                    |                                   |
                    v                                   v
              SQL Analytics                       AI Incident
                                                   Summary Table

                         +-------------------+
                         |   APACHE AIRFLOW  |
                         |   ORCHESTRATION   |
                         +-------------------+

---

# Key Takeaway

FinStream combines:

    Data Engineering
          +
    Cloud Data Lake
          +
    Batch Processing
          +
    Real-Time Streaming
          +
    Data Quality
          +
    Machine Learning
          +
    Generative AI
          +
    Workflow Orchestration

into a single end-to-end financial data platform.

The platform demonstrates how raw financial data can move from ingestion to analytics and finally to AI-powered interaction and incident intelligence.

---

## Author

Harshit

FinStream — Financial Data Engineering + AI Platform
