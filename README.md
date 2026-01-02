# 🧾 PySpark Retail ETL Pipeline

## 📌 Overview
This project implements an **end-to-end batch ETL pipeline using PySpark** to process retail transaction data.  
The pipeline ingests raw CSV data, performs data cleaning and transformations, computes business-level aggregations, and stores the results in **optimized Parquet format**.

The project is designed to demonstrate **core Apache Spark concepts** such as:
- Lazy evaluation
- DAG execution
- Transformations vs Actions
- Aggregations and grouping
- Performance optimizations using caching

---

## 🧠 Business Use Case
Analyze retail transaction data to generate:
- **Daily revenue trends**
- **Top-selling product by revenue**

These insights help stakeholders understand sales performance and product contribution.

---

## 🛠️ Tech Stack
- **Apache Spark (PySpark)**
- **Python 3**
- **Parquet (columnar storage)**

All tools and libraries used are **open-source and free**.

---

## 📂 Project Structure
````markdown
pyspark-retail-etl/
│
├── data/
│   └── data.csv
│
├── output/
│   ├── daily-revenue/
│   └── top-selling-products/
│
├── lib/
│   └── logger.py
│
├── transform_retail_etl.py
│
└── README.md
