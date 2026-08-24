# Rekrutacja

An end-to-end data engineering project built with **Databricks, PySpark, Spark Structured Streaming, and Delta Lake**.

The project implements a **Medallion Architecture** to ingest, validate, clean, quarantine, and transform claims data into analytics-ready datasets.

## Architecture

```text
                    CSV source
                        │
                        ▼
                 ┌─────────────┐
                 │    BRONZE   │
                 │ Raw claims  │
                 └──────┬──────┘
                        │
                        ▼
                 ┌─────────────┐
                 │    SILVER   │
                 │ Validation  │
                 │ Cleaning    │
                 └──────┬──────┘
                        │
               ┌────────┴────────┐
               ▼                 ▼
        Valid records       Invalid records
               │                 │
               ▼                 ▼
        Silver claims       Quarantine
               │
               ▼
          ┌───────────┐
          │   GOLD    │
          │ Analytics │
          └───────────┘

```

##Data Quality

Data quality is handled primarily in the Silver layer.

The pipeline is designed to:
* validate incoming records
* record validation errors
* identify valid and invalid records
* send invalid records to quarantine
* allow only valid records to continue downstream

This creates a clear separation between data ingestion, data quality, and analytics.

##Streaming

The pipeline uses Spark Structured Streaming with Delta Lake.

Streaming checkpoints are stored separately from the data tables and are not committed to Git.

The pipeline uses _availableNow_ processing to process currently available source data while retaining Structured Streaming semantics.

##Tables

The pipeline creates separate datasets for each layer.

Bronze -> Raw claims

Silver -> Validated claims

Quarantine -> Invalid claims

Gold -> Analytics-ready claims

The actual Databricks catalog/schema/table names are environment-specific and are configured in the notebooks.

##Running the Pipeline

The notebooks are designed to run in a Databricks environment with access to the required source data, volumes, and Delta tables. The pipeline is a *simulation of a streaming* and does not run continuously. 

##Configuration

Environment-specific paths and table names are defined in the notebooks. They are stored in the configured Databricks catalog and schema.

Streaming checkpoint locations are kept separate from the Git repository.

##Future Improvements

Possible future improvements include:

* string quality checks, e.g. valid state names
* extracting reusable transformations into a src/ package
* adding data-quality metrics
* adding pipeline monitoring and logging
* adding CI/CD deployment to Databricks
* parameterizing environments such as development and production
* adding orchestration with Databricks Workflows