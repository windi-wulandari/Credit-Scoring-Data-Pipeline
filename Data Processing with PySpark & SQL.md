# **Advanced Stage: PySpark Implementation and Data Analysis**

Following successful AWS and Databricks setup, we proceed with data analysis implementation using PySpark and Spark SQL.

## **1. SparkSession Initialization**

Environment used:
```python
SparkSession - hive
SparkContext
Spark UI
Version: v3.3.2
Master: local[8]
AppName: Databricks Shell
```

Session creation:
```python
spark = SparkSession.builder.appName("Credit Scoring EDA").getOrCreate()
```

SparkSession serves as the entry point for all Spark functionality. The `local[8]` configuration indicates that Spark will use 8 local threads for parallel processing.

## **2. AWS S3 Integration**

Data reading implementation from S3:
```python
credit_scoring_df = spark.read.schema(credit_scoring_schema) \
    .format("csv") \
    .option("header", "true") \
    .load("s3://windiwwd-projects/credit_scoring.csv")
```

In this implementation, we use:
- `.schema()`: Defines the expected data structure
- `.format("csv")`: Specifies input file format
- `.option("header", "true")`: Indicates that CSV file has headers
- `.load()`: Reads data from S3 bucket

**Important Note**: There is a difference in record count from 20,000 to 276,483 after loading. This needs further investigation to ensure data integrity and identify possible ETL process issues.

## **3. Data Preprocessing**

### **Duplicate Data Handling**
Before proceeding with analysis, duplicate data checking and handling is performed. This is important for:
- Ensuring analysis accuracy
- Avoiding bias in statistical calculations
- Maintaining overall data integrity
- Ensuring each unique case is properly recorded

### **Missing Values Handling**
For handling missing values, two different strategies are applied based on data type:

**For Categorical Data**:
- Using mode value (most frequently occurring value)
- This strategy is chosen because it:
  - Maintains existing data distribution
  - Avoids introduction of invalid new categories
  - Reflects general tendencies in the data

**For Numerical Data**:
- Using median value for each column
- This strategy is chosen because it:
  - Is more robust against outliers compared to mean
  - Better maintains data distribution
  - Is not affected by extreme values

## **4. Spark SQL Implementation**

```python
credit_scoring_df.createOrReplaceTempView("credit_data")
```

This temporary view is crucial because it:
- Enables use of standard SQL syntax for data analysis
- Facilitates transition for SQL-familiar team members
- Enables better query optimization by Spark engine
- Is temporary and only available during the session

Example of Spark SQL implementation for analysis:
```sql
SELECT
    NAME_CONTRACT_TYPE,
    SUM(CASE WHEN TARGET = 0 THEN 1 ELSE 0 END) AS target_0_count,
    SUM(CASE WHEN TARGET = 1 THEN 1 ELSE 0 END) AS target_1_count,
    COUNT(*) AS total_count,
    ROUND((SUM(CASE WHEN TARGET = 0 THEN 1 ELSE 0 END) / COUNT(*)) * 100, 2) AS target_0_percentage,
    ROUND((SUM(CASE WHEN TARGET = 1 THEN 1 ELSE 0 END) / COUNT(*)) * 100, 2) AS target_1_percentage
FROM credit_data
GROUP BY NAME_CONTRACT_TYPE
```

This query performs:
- Data aggregation by contract type
- Count calculation for each target (0: non-default, 1: default)
- Percentage calculation to understand default rate distribution
- Rounding percentage results to 2 decimals for readability

## **5. Visualization Preparation**

```python
contract_type_df = contract_type.toPandas()
```

Conversion to Pandas is necessary because:
- Pandas has better integration with Python visualization libraries like matplotlib and seaborn
- Data is already aggregated so size is smaller and more efficient to process in memory
- Enables more flexible data manipulation for visualization purposes

## **6. Analysis Results and Visualization**

**Figure 1: Loan Type Analysis**

![Gambar 1](https://drive.google.com/uc?id=1bOBlRk3QsL_6UNydwSejLE_abwLur--q)

**Analysis**: The analysis shows a significant disparity in default rates between cash and revolving loans. Cash loans record 1490 default cases, substantially higher than revolving loans with only 88 cases. This indicates that customers with cash loans have a substantially higher default risk, possibly due to factors such as larger loan amounts or stricter terms and conditions.

**Figure 2: Gender Analysis**
 
 ![Gambar 2](https://drive.google.com/uc?id=1mvG71G_xok0m_KiausaDy4H46Ra2LG77)

**Analysis**: The data reveals significant differences in default patterns based on gender. Male customers show a default rate of 10.06%, almost 1.5 times higher than female customers at 6.76%. This finding could be an important consideration in the credit scoring process, although other factors that might influence this difference, such as income levels, occupation types, or other socio-economic factors, should also be considered.

**Figure 3: Car Ownership Analysis**
 
 ![Gambar 3](https://drive.google.com/uc?id=10uDWbIN55k3YZfgbJE_dgWoBYI1DxqV-)

**Analysis**: Car ownership shows a correlation with default rates, although the difference is not substantial. Customers with cars show a default rate of 7.52%, while customers without cars are slightly higher at 8.08%. This 0.56% difference might indicate that asset ownership like cars could be one indicator of financial stability, although not significantly strong as a single predictor of default risk.

**Figure 4: Number of Children Analysis**

![Gambar 4](https://drive.google.com/uc?id=1OLp21nU4lcKWenzypTSE4dEkojRbvI99)

**Analysis**: The data shows an interesting correlation between number of children and default rates. There is a trend of increasing default risk as the number of children increases, with the lowest default rate among customers with no children (7.29%), progressively increasing for those with 1 child (8.95%) and 2 children (9.73%). A significant increase is seen in families with 4-5 children, where default rates reach 24.14% and 33.33%. However, it should be noted that the sample size for families with >3 children is relatively small, so these high percentages might not be fully representative. Data for families with 6-8 children shows a 0% default rate, but with very small sample sizes (1-2 cases), making it unsuitable for drawing strong conclusions.

**Note**: While visualizations in this project were not exhaustively implemented as the main focus was on cloud setup and code implementation, fundamental analysis was thoroughly conducted to understand the influence of each variable on credit scoring. Complete analysis can be found in the project notebook.