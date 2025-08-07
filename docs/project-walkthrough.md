# Project Walkthrough

This project demonstrates a modular and reusable Data Quality (DQ) validation framework using PySpark in a Databricks environment. It integrates with Azure Data Factory and CI/CD pipelines for production-grade ingestion workflows.

## Key Components

- **datasets/**: Sample data with edge cases and DQ failures
- **dq-framework/**: Reusable PySpark code for null checks, duplicate checks, range validation, and referential integrity
- **notebooks/**: Databricks-compatible notebooks for orchestrating DQ jobs
- **adf-cicd/**: GitHub Actions YAML template for automating ADF deployments
- **docs/**: Project documentation and use cases

## How to Use

1. Upload sample data to Databricks or Synapse
2. Run the `dq_batch_validation.py` notebook
3. Integrate PySpark DQ logic into your data ingestion pipelines
4. Automate pipeline deployment using GitHub Actions
