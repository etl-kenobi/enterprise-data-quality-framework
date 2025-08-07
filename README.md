# 🧪 Azure Batch Data Quality Validation Project

This project demonstrates a complete data quality validation pipeline using **Azure Data Factory**, **Azure SQL**, **Azure Data Lake Storage**, and **Azure Databricks (PySpark)**.

---

## 📌 Use Case

Customer data in `.csv` format is ingested from **Azure Data Lake** into a **CustomerStaging** table using **ADF**. Then, **PySpark**-based **Data Quality Checks** are performed in **Azure Databricks**, and valid and rejected records are separated and stored in Azure SQL.

---

## 🛠️ Tech Stack

- Azure Data Lake Storage (ADLS Gen2)
- Azure Data Factory (ADF)
- Azure SQL Database
- Azure Databricks (PySpark)
- PySpark DataFrame API

---

## 📂 Data Flow Overview

1. **Raw Data Source** in ADLS (`customers.csv`, `transactions.csv`, etc.)
2. **ADF Copy Activity** loads data into `CustomerStaging` table in Azure SQL.
3. **Databricks Notebook** validates the data using PySpark:
   - Null checks
   - Duplicate checks
   - Email pattern validation
4. **Clean Data** → stored in `CustomerValidated` table.
5. **Invalid Data** → stored in `CustomerRejected` table with reason.

---

## 🖼️ Architecture Screenshots

### 1. Azure Data Lake Source (Raw CSVs)
![Datasets](./Datasets.png)

### 2. ADF Ingestion Pipeline
![ADF Pipeline](./ADF ingest pipeline.png)

### 3. Data in `CustomerStaging` Table
![Staging Table](./Data at sink.png)

### 4. Databricks Notebook with Validation Logic
![Databricks Notebook](./DataBricks_validations.png)

### 5. Final Validated Records in `CustomerValidated`
![Cleaned Output](./Cleansed_Data.png)

---

## ✅ Validation Checks Performed

| Check Type         | Description |
|--------------------|-------------|
| Null Check         | Verifies `customer_id`, `name`, `email` are not null |
| Duplicate Check    | Detects duplicate `customer_id`s |
| Email Pattern Check| Validates format using regex `^[\w\.-]+@[\w\.-]+\.\w+$` |

---

## 📁 Output Tables in Azure SQL

- `CustomerStaging`: Raw ingested data
- `CustomerValidated`: Passed records
- `CustomerRejected`: Failed records with reason

---

## 👤 Author

**Rahul Gurjar** | Data Engineer | [Azure | PySpark | ADF | SQL | Delta Lake]