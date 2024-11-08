# **Credit Scoring Data Pipeline with AWS S3 and Databricks**

# **Overview**
This project demonstrates the implementation of a simple data pipeline using AWS S3 and Databricks, with a primary focus on cloud infrastructure setup and the utilization of PySpark and SQL Spark for data processing. The dataset used is credit scoring data from Kaggle, modified to accommodate computational limitations and project requirements.

# **Project Architecture**


![Deskripsi Gambar](https://drive.google.com/uc?export=view&id=1Wry03bigxG_e8e-SCZNGgHLnTKJPUJ-H)


# **Main Focus**
This project emphasizes:
- AWS S3 implementation as a storage solution
- Integration between AWS S3 and Databricks
- PySpark and SQL Spark usage for data processing
- Data cleansing and visualization

## **Dataset**
The dataset used in this project comes from the [Kaggle Credit Scoring Dataset](https://www.kaggle.com/competitions/home-credit-credit-risk-model-stability/discussion/473704). This [dataset](https://github.com/windi-wulandari/Credit-Scoring-Data-Pipeline/tree/main/dataset) has been modified and can be accessed in the dataset file within this repository to accommodate:
- Computational limitations
- Technical pipeline demonstration focus
- PySpark and SQL Spark implementation examples

# **Detailed Documentation**
The project is divided into several sections that can be explored further:
1. **[AWS S3 Setup](https://github.com/windi-wulandari/Credit-Scoring-Data-Pipeline/blob/main/AWS%20S3%20Setup.md)**
   - Bucket configuration
   - Credential setup
   - Bucket policy configuration for secure access
   - Dataset upload

2. **[Databricks Setup](https://github.com/windi-wulandari/Credit-Scoring-Data-Pipeline/blob/main/Databricks%20Setup.md)**
   - Creation of computation resources
   - AWS S3 connection
   - Workspace setup

3. **[Data Processing with PySpark & SQL](https://github.com/windi-wulandari/Credit-Scoring-Data-Pipeline/blob/main/Credit_scoring_notebook.py)**
   - Schema creation and definition
   - DataFrame structure introduction
   - Data cleaning
   - Basic EDA
   - Visualization

## **Prerequisites**
- AWS Free Tier Account
- Databricks Community Edition or paid version account
- Advanced understanding of:
  - Python programming
  - SQL query and data manipulation
  - PySpark DataFrame operations
  - Spark SQL syntax and functions
  - Data Visualization
- Basic understanding of:
  - Cloud Storage concepts

# **Project Limitations**
It's important to note that this project has several limitations:
1. The dataset has been simplified to focus on technical aspects
2. The analysis performed is basic and not in-depth
3. Does not include:
   - Comprehensive credit analysis
   - Real-time processing

## **Learning Objectives**
After exploring this project, you should be able to:
1. Implement a basic data pipeline using AWS S3
2. Integrate AWS S3 with Databricks
3. Perform data processing using PySpark and SQL Spark
4. Understand fundamentals of cloud storage and big data processing
