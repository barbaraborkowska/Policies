# Rekrutacja

An end-to-end data engineering project built with Databricks, PySpark, Spark Structured Streaming, and Delta Lake.

The project implements a **Medallion Architecture** to ingest, validate, clean, quarantine, and transform claims data into analytics-ready datasets.

## Architecture

```text
                         CSV source
                             │
                             ▼
                     ┌─────────────┐
                     │   BRONZE    │
                     │ Raw claims  │
                     │ + metadata  │
                     └──────┬──────┘
                            │
                            ▼
                     ┌─────────────┐
                     │   SILVER    │
                     │ Validation  │
                     │  & Cleaning │
                     └──────┬──────┘
                            │
                   ┌────────┴────────┐
                   ▼                 ▼
             Valid records     Invalid records
                   │                 │
                   ▼                 ▼
            Silver claims       Quarantine
                   │
                   ▼
             ┌───────────┐
             │   GOLD    │
             │ Business  │
             │ Analytics │
             └───────────┘

```

###Bronze

The Bronze layer contains the raw claims data ingested from the CSV source.

The ingestion is implemented using **Spark Structured Streaming and Delta Lake**. The source CSV is processed using *availableNow=True*, which simulates a streaming workload while allowing the project to be run as a finite pipeline.

Technical metadata is added during ingestion, including:

* ingestion timestamp
* source information

###Silver

The Silver layer is responsible for **data validation and cleaning**.

The pipeline validates incoming claims against business and data-quality rules, including checks for:

* required fields and NULL values
* negative or invalid monetary values
* invalid dates
* values outside allowed ranges
* inconsistencies between related columns
* other domain-specific validation rules

Validation errors are recorded in a dedicated *validation_error* column. A boolean *is_valid* flag is then used to separate valid and invalid records.

####Handling invalid records

Invalid records are not discarded.

Instead, they are written to a separate **Quarantine** dataset. This approach preserves problematic source records for investigation and debugging while preventing invalid data from being propagated to the Gold layer.

Only records that pass validation are written to the Silver claims dataset and continue downstream.

###Gold

The Gold layer contains business-oriented datasets designed for analytical use.

The project creates three analytical datasets:

* Fraud analysis – aggregates claims by incident type and incident severity and provides claim counts, fraudulent claim counts, claim amounts, and fraud rates.
* [Gold table 2] – [brief description of the business purpose].
* [Gold table 3] – [brief description of the business purpose].

The Gold datasets are derived only from validated Silver records.

##Streaming

The pipeline uses **Spark Structured Streaming with Delta Lake** for the Bronze and Silver processing stages.

The provided CSV file is used as a simulated streaming source. *availableNow=True* processes all currently available input and then terminates the stream, making the pipeline reproducible without requiring a continuously running source.

Streaming checkpoints are stored separately from the data tables and are not committed to Git.

Gold processing is currently implemented as analytical transformations over the validated Silver data .


##Data Quality

Data quality is handled primarily in the Silver layer.

The pipeline is designed to:
* Preserve the raw source data in Bronze.
* Validate records in Silver.
* Record validation errors and quarantine the records.
* Allow only valid records to reach Gold.


##Tables

The pipeline creates separate datasets for each layer.

Bronze -> Raw claims

Silver -> Validated claims

Quarantine -> Invalid claims

Gold -> Analytics-ready claims

The actual Databricks catalog/schema/table names are environment-specific and are configured in the notebooks.

##Running the Pipeline

The notebooks are designed to run in a **Databricks** environment with access to the required source data, volumes, and Delta tables. 

The pipeline is a **streaming simulation** rather than a continuously running production stream. The CSV source is processed with Structured Streaming and *availableNow=True*.

A typical execution order is:

1. Run the Bronze notebook to ingest the source CSV.
2. Run the Silver notebook to validate and clean the Bronze data.
3. Run the Gold notebook(s) to create the analytical datasets.
4. Run the unit tests to verify the transformation and validation logic.

##Configuration

Environment-specific paths and table names are defined in the notebooks and stored in the configured Databricks catalog and schema.

Streaming checkpoint locations are kept separate from the Git repository.

## Testing

The project includes basic tests for the Silver and Gold layers.

* **Silver tests** verify that valid records pass validation and invalid records are correctly identified with the expected validation errors.
* **Gold tests** verify the fraud aggregation logic, including claim counts, fraudulent claim counts, average and total claim amounts, and fraud rate calculations.

The tests use small, controlled test datasets and assertions to verify the expected results.

##Design Decisions
#####Quarantine instead of dropping invalid records

Invalid records are retained in a dedicated Quarantine dataset rather than being silently discarded. This makes data-quality issues traceable and allows problematic records to be investigated or reprocessed.

##### *availableNow=True*

The provided CSV is a static file, so *availableNow=True* is used to simulate streaming ingestion while keeping the project deterministic and easy to execute.

#####Delta Lake

Delta Lake is used as the storage format between pipeline layers to provide reliable table storage and compatibility with Structured Streaming.

#####Incremental processing
Gold is currently generated from the validated Silver data using analytical transformations.

A production implementation could use Delta Change Data Feed (CDF) or Databricks materialized views to maintain Gold datasets incrementally. These approaches were not implemented here because the project was developed in Databricks Free Edition and the initial table setup was not configured for CDF.

The current implementation prioritizes clear business transformations and reproducibility within the available environment.


##Future Improvements
Possible future improvements include:

* implementing incremental Gold processing using Delta Change Data Feed
* adding additional string-quality checks, e.g. valid state names
* extracting reusable transformations into a src/ package
* adding data-quality metrics and reporting
* adding pipeline monitoring and logging
* adding CI/CD deployment to Databricks
* parameterizing environments such as development and production
* adding orchestration with Databricks Workflows

