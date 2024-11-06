# Databricks notebook source
spark

# COMMAND ----------

# MAGIC %md
# MAGIC # Spark DataFrame

# COMMAND ----------

from pyspark.sql.types import StructField, StructType, IntegerType, StringType, BooleanType, DoubleType, DateType, DecimalType
from pyspark.sql.functions import col, when, sum, avg, row_number 
from pyspark.sql.window import Window
from pyspark.sql import functions as F
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

# COMMAND ----------

from pyspark.sql import SparkSession 

#create session
spark = SparkSession.builder.appName("Credit Scoring EDA").getOrCreate()

# COMMAND ----------

credit_scoring_schema = StructType([
    StructField("SK_ID_CURR", IntegerType(), True),
    StructField("TARGET", IntegerType(), True),
    StructField("NAME_CONTRACT_TYPE", StringType(), True),
    StructField("CODE_GENDER", StringType(), True),
    StructField("FLAG_OWN_CAR", StringType(), True),
    StructField("FLAG_OWN_REALTY", StringType(), True),
    StructField("CNT_CHILDREN", IntegerType(), True),
    StructField("AMT_INCOME_TOTAL", DoubleType(), True),
    StructField("AMT_CREDIT", DoubleType(), True),
    StructField("AMT_ANNUITY", DoubleType(), True),
    StructField("AMT_GOODS_PRICE", DoubleType(), True),
    StructField("NAME_TYPE_SUITE", StringType(), True),
    StructField("NAME_INCOME_TYPE", StringType(), True),
    StructField("NAME_EDUCATION_TYPE", StringType(), True),
    StructField("NAME_FAMILY_STATUS", StringType(), True),
    StructField("NAME_HOUSING_TYPE", StringType(), True),
    StructField("DAYS_BIRTH", IntegerType(), True),
    StructField("DAYS_EMPLOYED", IntegerType(), True),
    StructField("OCCUPATION_TYPE", StringType(), True),
    StructField("CNT_FAM_MEMBERS", IntegerType(), True),
    StructField("REGION_RATING_CLIENT", IntegerType(), True),
    StructField("EXT_SOURCE_1", DoubleType(), True),
    StructField("EXT_SOURCE_2", DoubleType(), True),
    StructField("EXT_SOURCE_3", DoubleType(), True)
])


# COMMAND ----------

# Read the data from S3 using the defined schema
credit_scoring_df = spark.read.schema(credit_scoring_schema) \
    .format("csv") \
    .option("header", "true") \
    .load("s3://windiwwd-projects/credit_scoring.csv")

# Show the first few rows to verify
credit_scoring_df.show()


# COMMAND ----------

# Show schema
credit_scoring_df.printSchema()

# COMMAND ----------

# Displaying descriptive statistics of numerical columns
credit_scoring_df.describe().show()

# COMMAND ----------

# MAGIC %md
# MAGIC # Data Cleansing

# COMMAND ----------

# MAGIC %md
# MAGIC ## Handle Data Duplicates

# COMMAND ----------

print(f"Row Count: {credit_scoring_df.distinct().count()}")


# COMMAND ----------

# Calculate the number of original data rows
n1 = credit_scoring_df.count()
print("Number of original data rows: ", n1)

# Calculate the number of rows after removing duplicates
n2 = credit_scoring_df.dropDuplicates().count()
print("Number of data rows after deleting duplicated data: ", n2)

# Calculate the number of duplicated rows
n3 = n1 - n2
print("Number of duplicated data: ", n3)


# COMMAND ----------

unique_credit_scoring_df = credit_scoring_df.dropDuplicates()


# COMMAND ----------

n2 = unique_credit_scoring_df.count()
print("number of data rows after deleting duplicated data: ", n2)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Handle Missing Values

# COMMAND ----------

# Grouping columns into categorical and numerical
categorical_columns = [
    "NAME_CONTRACT_TYPE", "CODE_GENDER", "FLAG_OWN_CAR", "FLAG_OWN_REALTY",
    "NAME_TYPE_SUITE", "NAME_INCOME_TYPE", "NAME_EDUCATION_TYPE", 
    "NAME_FAMILY_STATUS", "NAME_HOUSING_TYPE", "OCCUPATION_TYPE"
]

numerical_columns = [
    "CNT_CHILDREN", "AMT_INCOME_TOTAL", "AMT_CREDIT", "AMT_ANNUITY", 
    "AMT_GOODS_PRICE", "DAYS_BIRTH", "DAYS_EMPLOYED", "CNT_FAM_MEMBERS", 
    "REGION_RATING_CLIENT", "EXT_SOURCE_1", "EXT_SOURCE_2", "EXT_SOURCE_3"
]

# COMMAND ----------

# Count the number of missing values in each column after removing duplicates
missing_values_unique = unique_credit_scoring_df.select([
    F.count(F.when(F.col(c).isNull(), 1)).alias(c) for c in categorical_columns + numerical_columns
])

# Display the number of missing values
print("Number of missing values in each column after removing duplicates:")
missing_values_unique.show()


# COMMAND ----------

# Imputing missing values for categorical columns with mode
for column in categorical_columns:
    mode = unique_credit_scoring_df.groupBy(column).count().orderBy(F.desc("count")).first()[0]  # Get mode
    unique_credit_scoring_df = unique_credit_scoring_df.fillna({column: mode})

# Imputing missing values for numerical columns with median
for column in numerical_columns:
    median = unique_credit_scoring_df.approxQuantile(column, [0.5], 0.01)[0]  # Get median
    unique_credit_scoring_df = unique_credit_scoring_df.fillna({column: median})

# Rename dataframe back to credit_scoring_df
credit_scoring_df = unique_credit_scoring_df

# COMMAND ----------

# Displaying the number of missing values after imputation
missing_values_after_imputation = credit_scoring_df.select([
    F.count(F.when(F.col(c).isNull(), 1)).alias(c) for c in categorical_columns + numerical_columns
])

print("Number of missing values after imputation:")
missing_values_after_imputation.show()


# COMMAND ----------

# MAGIC %md
# MAGIC # Exploratory Data Analysis (EDA) Using Spark SQL

# COMMAND ----------

# Creating a temporary view for SQL queries
credit_scoring_df.createOrReplaceTempView("credit_data")

# COMMAND ----------

# MAGIC %md
# MAGIC ## How Does Contract Type Relate to Default Risk?

# COMMAND ----------

contract_type = spark.sql("""
SELECT 
    NAME_CONTRACT_TYPE,
    SUM(CASE WHEN TARGET = 0 THEN 1 ELSE 0 END) AS target_0_count,
    SUM(CASE WHEN TARGET = 1 THEN 1 ELSE 0 END) AS target_1_count,
    COUNT(*) AS total_count,
    ROUND((SUM(CASE WHEN TARGET = 0 THEN 1 ELSE 0 END) / COUNT(*)) * 100, 2) AS target_0_percentage,
    ROUND((SUM(CASE WHEN TARGET = 1 THEN 1 ELSE 0 END) / COUNT(*)) * 100, 2) AS target_1_percentage
FROM credit_data
GROUP BY NAME_CONTRACT_TYPE
""")

contract_type.show()


# COMMAND ----------

# Convert from PySpark DataFrame to Pandas DataFrame
contract_type_df = contract_type.toPandas()

# Set background and color palette
sns.set(rc={"axes.facecolor": "#F5F5F3", "figure.facecolor": "#F5F5F3"})
neutral_palette = ["#3E2C41", "#6E4B50", "#A26769", "#D9B5A6", "#F5E0C3", "#FFF1E6"]

# Rename columns for a more informative legend
contract_type_df = contract_type_df.rename(columns={
    "target_0_count": "No Default",
    "target_1_count": "Default"
})

# Plotting bar chart
fig, ax = plt.subplots(figsize=(8, 5))
contract_type_df.plot(kind="bar", x="NAME_CONTRACT_TYPE", stacked=True, 
                      color=neutral_palette[:2], edgecolor=neutral_palette[0], ax=ax)

# Adding title and subtitle with analysis summary
ax.set_title("Distribution of Contract Types by Credit Status", fontsize=15, fontweight='bold', color='#3E2C41', pad=50)
fig.suptitle("Cash loans show a significantly higher default risk than Revolving loans.\n"
             "This indicates that customers with Cash loans are more likely to default.",
             fontsize=10, color='#6E4B50', style='italic', x=0.55, y=0.80)

# Adjust x and y labels
ax.set_xlabel("Type of Contract", fontsize=12, color='#3E2C41')
ax.set_ylabel("Count", fontsize=12, color='#3E2C41')

# Adding annotations to each bar
for bar in ax.containers:
    ax.bar_label(bar, label_type='center', color='#FFF1E6', fontsize=9)

# Adjust legend without a specific title
ax.legend(labels=["No Default", "Default"], loc="upper left", frameon=False)

plt.xticks(rotation=0)
plt.tight_layout()
plt.show()


# COMMAND ----------

# MAGIC %md
# MAGIC The data shows that *Cash loans* have a significantly higher default count (1490) compared to *Revolving loans* (88). This suggests that customers with *Cash loans* are at a higher risk of default than those with *Revolving loans*.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Is There a Gender Difference in Default Risk?

# COMMAND ----------

gender_target = spark.sql("""
SELECT 
    CODE_GENDER,
    SUM(CASE WHEN TARGET = 0 THEN 1 ELSE 0 END) AS target_0_count,
    SUM(CASE WHEN TARGET = 1 THEN 1 ELSE 0 END) AS target_1_count,
    COUNT(*) AS total_count,
    ROUND((SUM(CASE WHEN TARGET = 0 THEN 1 ELSE 0 END) / COUNT(*)) * 100, 2) AS target_0_percentage,
    ROUND((SUM(CASE WHEN TARGET = 1 THEN 1 ELSE 0 END) / COUNT(*)) * 100, 2) AS target_1_percentage
FROM credit_data
GROUP BY CODE_GENDER
""")

gender_target.show()


# COMMAND ----------

# Convert from PySpark DataFrame to Pandas DataFrame
gender_df = gender_target.toPandas()

# Set aesthetic background and color palette
sns.set(rc={"axes.facecolor": "#F5F5F3", "figure.facecolor": "#F5F5F3"})
neutral_palette = ["#3E2C41", "#6E4B50"]

# Plotting stacked bar chart
fig, ax = plt.subplots(figsize=(8, 5))
gender_df.plot(kind="bar", x="CODE_GENDER", y=["target_0_count", "target_1_count"],
               stacked=True, color=neutral_palette, edgecolor=neutral_palette[0], ax=ax)

# Adding title and subtitle with analysis summary
ax.set_title("Credit Default Distribution by Gender", fontsize=15, fontweight='bold', color='#3E2C41', pad=50)
fig.suptitle("Males show a higher percentage of default compared to females.\n"
             "This indicates a slightly higher risk of default among male customers.",
             fontsize=10, color='#6E4B50', style='italic', x=0.56, y=0.82)

# Adjust x and y labels
ax.set_xlabel("Gender", fontsize=12, color='#3E2C41')
ax.set_ylabel("Count", fontsize=12, color='#3E2C41')

# Adding percentage annotations in the center of each bar
for i, row in gender_df.iterrows():
    # Calculate the middle position of target_0 and target_1
    middle_0 = row['target_0_count'] / 2
    middle_1 = row['target_0_count'] + (row['target_1_count'] / 2)
    
    # Percentage annotation for target 0 in the center of the first bar
    ax.annotate(f"{row['target_0_percentage']:.2f}%", 
                xy=(i, middle_0), 
                ha='center', va='center', 
                color="white", fontsize=9, fontweight="bold")
    
    # Percentage annotation for target 1 in the center of the second bar
    ax.annotate(f"{row['target_1_percentage']:.2f}%", 
                xy=(i, middle_1), 
                ha='center', va='center', 
                color="white", fontsize=9, fontweight="bold")

# Adjust legend
ax.legend(["No Default", "Default"], loc="upper right", frameon=False)

plt.xticks(rotation=0)
plt.tight_layout()
plt.show()


# COMMAND ----------

# MAGIC %md
# MAGIC The data indicates that male customers have a higher default percentage compared to female customers. Males show a default rate of 10.06%, whereas females have a lower default rate of 6.76%. This suggests a slightly higher default risk among male customers.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Does Car Ownership Impact Default Risk?

# COMMAND ----------

car_target = spark.sql("""
SELECT 
    FLAG_OWN_CAR,
    SUM(CASE WHEN TARGET = 0 THEN 1 ELSE 0 END) AS target_0_count,
    SUM(CASE WHEN TARGET = 1 THEN 1 ELSE 0 END) AS target_1_count,
    COUNT(*) AS total_count,
    ROUND((SUM(CASE WHEN TARGET = 0 THEN 1 ELSE 0 END) / COUNT(*)) * 100, 2) AS target_0_percentage,
    ROUND((SUM(CASE WHEN TARGET = 1 THEN 1 ELSE 0 END) / COUNT(*)) * 100, 2) AS target_1_percentage
FROM credit_data
GROUP BY FLAG_OWN_CAR
""")

car_target.show()


# COMMAND ----------

# Set aesthetic background and color palette
sns.set(rc={"axes.facecolor": "#F5F5F3", "figure.facecolor": "#F5F5F3"})
colors = ["#3E2C41", "#6E4B50"]

# Data for pie chart
default_car_data = [
    car_df.loc[car_df['FLAG_OWN_CAR'] == 'Y', 'target_1_count'].values[0],
    car_df.loc[car_df['FLAG_OWN_CAR'] == 'N', 'target_1_count'].values[0]
]

# Labels for pie chart
labels = ['With Car', 'Without Car']

# Create pie chart with a slight explosion for the smaller slice
explode = (0, 0.1)  # Only "Without Car" slice is exploded

# Create pie chart
fig, ax = plt.subplots(figsize=(8, 6))
wedges, texts, autotexts = ax.pie(default_car_data, labels=labels, colors=colors, startangle=90, 
                                   autopct='%1.1f%%', explode=explode, 
                                   wedgeprops=dict(edgecolor='w'))

# Set the color of percentage text to white
for autotext in autotexts:
    autotext.set_color('white')

# Add title and subtitle with analysis conclusion
ax.set_title("Credit Default Distribution by Car Ownership", fontsize=15, fontweight='bold', color='#3E2C41', pad=50, x=0.55)
fig.suptitle("Customers without a car have a higher default percentage compared to those with a car.\n"
             "This indicates a higher risk of default among customers without vehicle ownership.",
             fontsize=10, color='#6E4B50', style='italic', x=0.56, y=0.98)

plt.axis('equal')  # Equal aspect ratio ensures that pie is drawn as a circle.
plt.show()


# COMMAND ----------

# MAGIC %md
# MAGIC From the analysis of the car ownership data, it is observed that customers with a car (Y) exhibit a default percentage of 7.52%, while customers without a car (N) have a default percentage of 8.08%. Although both default percentages are relatively low, customers without a car show a slightly higher risk of default compared to those with a car.

# COMMAND ----------

# MAGIC %md
# MAGIC ## How Does the Number of Children Affect Default Risk?

# COMMAND ----------

children_target = spark.sql("""
SELECT 
    CNT_CHILDREN,
    SUM(CASE WHEN TARGET = 0 THEN 1 ELSE 0 END) AS target_0_count,
    SUM(CASE WHEN TARGET = 1 THEN 1 ELSE 0 END) AS target_1_count,
    COUNT(*) AS total_count,
    ROUND((SUM(CASE WHEN TARGET = 0 THEN 1 ELSE 0 END) / COUNT(*)) * 100, 2) AS target_0_percentage,
    ROUND((SUM(CASE WHEN TARGET = 1 THEN 1 ELSE 0 END) / COUNT(*)) * 100, 2) AS target_1_percentage
FROM credit_data
GROUP BY CNT_CHILDREN
ORDER BY CNT_CHILDREN
""")

children_target.show()


# COMMAND ----------

# Set Seaborn theme
sns.set(rc={"axes.facecolor": "#F5F5F3", "figure.facecolor": "#F5F5F3"})

# Retrieve data from children_target
children_data = children_target.collect()

# Store data in lists
cnt_children = [row.CNT_CHILDREN for row in children_data]
target_0_count = [row.target_0_count for row in children_data]
target_1_count = [row.target_1_count for row in children_data]

# Calculate total count
total_count = np.array(target_0_count) + np.array(target_1_count)

# Calculate percentages
target_0_percentage = np.array(target_0_count) / total_count * 100
target_1_percentage = np.array(target_1_count) / total_count * 100

# Define colors for the stacked bar plot
color_non_default = "#3E2C41"  # Color for target 0 (Non-Default)
color_default = "#D9B5A6"       # Color for target 1 (Default)

# Create Stacked Bar Plot
fig, ax = plt.subplots(figsize=(10, 6))

# Plotting
bars_non_default = ax.bar(cnt_children, target_0_percentage, label='Non-Default', color=color_non_default)
bars_default = ax.bar(cnt_children, target_1_percentage, bottom=target_0_percentage, label='Default', color=color_default)

# Add title and subtitle with analysis summary
ax.set_title("Distribution of Credit Default by Number of Children", fontsize=15, fontweight='bold', color='#3E2C41', pad=60)
fig.suptitle("No significant difference in default rates from having no children to three children.\n"
             "The small counts for four or more children limit representativeness of this group.",
             fontsize=10, color='#6E4B50', style='italic', x=0.45, y=0.85)

# Add labels and legend
ax.set_xlabel('Number of Children', fontsize=12)
ax.set_ylabel('Percentage (%)', fontsize=12)
ax.set_xticks(cnt_children)
ax.set_xticklabels(cnt_children)

# Set x-axis limits to add extra space on the right
ax.set_xlim([min(cnt_children) - 0.5, max(cnt_children) + 0.50])  # Adds 1 unit of empty space

# Add legend outside the plot
ax.legend(loc='upper left', bbox_to_anchor=(1, 1))

# Set y-axis limits
plt.ylim(0, 100)

# Add labels with counts on each bar
for i in range(len(bars_non_default)):
    # Display count for non-default (0) inside the bar
    ax.text(bars_non_default[i].get_x() + bars_non_default[i].get_width()/2, 
            bars_non_default[i].get_height()/2,  # Place in center of the bar
            f'{target_0_count[i]}', ha='center', va='center', fontsize=10, color='white')

    # Display count for default (1) above the bar
    ax.text(bars_default[i].get_x() + bars_default[i].get_width()/2, 
            target_0_percentage[i] + bars_default[i].get_height() + 2,  # Above the bar
            f'{target_1_count[i]}', ha='center', va='bottom', fontsize=10, color='black')

plt.grid(axis='y', linestyle='--', alpha=0.7)  # Add grid for y-axis
plt.tight_layout()  # Adjust layout for better fit
plt.show()


# COMMAND ----------

# MAGIC %md
# MAGIC ## Does Real Estate Ownership Impact Default Risk?

# COMMAND ----------

realty_target = spark.sql("""
SELECT 
    FLAG_OWN_REALTY,
    SUM(CASE WHEN TARGET = 0 THEN 1 ELSE 0 END) AS target_0_count,
    SUM(CASE WHEN TARGET = 1 THEN 1 ELSE 0 END) AS target_1_count,
    COUNT(*) AS total_count,
    ROUND((SUM(CASE WHEN TARGET = 0 THEN 1 ELSE 0 END) / COUNT(*)) * 100, 2) AS target_0_percentage,
    ROUND((SUM(CASE WHEN TARGET = 1 THEN 1 ELSE 0 END) / COUNT(*)) * 100, 2) AS target_1_percentage
FROM credit_data
GROUP BY FLAG_OWN_REALTY
""")

realty_target.show()


# COMMAND ----------

# MAGIC %md
# MAGIC The data shows a slight difference in default rates between those who own real estate (REALTY = Y) and those who do not (REALTY = N). Among property owners, 7.74% defaulted, while non-property owners had a slightly higher default rate of 8.24%. However, the number of property owners is significantly higher (13,907 people compared to 6,093 people), meaning more non-defaulting individuals come from the property-owning group, despite the higher default percentage among non-owners. Therefore, realty ownership does not seem to have a significant impact on default risk when considering the total number of cases involved.

# COMMAND ----------

# MAGIC %md
# MAGIC ## How Does the Type of Suite Affect Default Risk?

# COMMAND ----------

type_suite = spark.sql("""
SELECT 
    NAME_TYPE_SUITE,
    SUM(CASE WHEN TARGET = 0 THEN 1 ELSE 0 END) AS target_0_count,
    SUM(CASE WHEN TARGET = 1 THEN 1 ELSE 0 END) AS target_1_count,
    COUNT(*) AS total_count,
    ROUND((SUM(CASE WHEN TARGET = 0 THEN 1 ELSE 0 END) / COUNT(*)) * 100, 2) AS target_0_percentage,
    ROUND((SUM(CASE WHEN TARGET = 1 THEN 1 ELSE 0 END) / COUNT(*)) * 100, 2) AS target_1_percentage
FROM credit_data
GROUP BY NAME_TYPE_SUITE
""")

type_suite.show()


# COMMAND ----------

# MAGIC %md
# MAGIC The data shows variability in default rates based on suite type. The "Spouse, partner" suite type has the lowest default rate at 6.67%, followed by "Group of people" and "Family" with default rates of 6.25% and 7.53%, respectively, all of which are relatively low. However, the "Unaccompanied" suite type has the largest number of individuals, with 16,243 people, and a default rate of 7.98%. While the "Spouse, partner" suite has the lowest default percentage, the number of individuals involved is much smaller compared to the "Unaccompanied" suite. Therefore, even though some suite types have low default percentages, the large number of individuals in the "Unaccompanied" category contributes significantly to the overall default cases.

# COMMAND ----------

# MAGIC %md
# MAGIC ## How Does Income Type Influence Credit Default Risk?

# COMMAND ----------

income_type = spark.sql("""
SELECT 
    NAME_INCOME_TYPE,
    SUM(CASE WHEN TARGET = 0 THEN 1 ELSE 0 END) AS target_0_count,
    SUM(CASE WHEN TARGET = 1 THEN 1 ELSE 0 END) AS target_1_count,
    COUNT(*) AS total_count,
    ROUND((SUM(CASE WHEN TARGET = 0 THEN 1 ELSE 0 END) / COUNT(*)) * 100, 2) AS target_0_percentage,
    ROUND((SUM(CASE WHEN TARGET = 1 THEN 1 ELSE 0 END) / COUNT(*)) * 100, 2) AS target_1_percentage
FROM credit_data
GROUP BY NAME_INCOME_TYPE
""")

income_type.show()


# COMMAND ----------

# MAGIC %md
# MAGIC The "State servant" and "Pensioner" income types exhibit the lowest default rates, at 5.24% and 5.32%, respectively, with significant total numbers of 1,356 and 3,572 individuals. On the other hand, the "Commercial associate" type has a slightly higher default rate of 7.48%, but with a substantial total count of 4,612 people. The "Working" income type, with the largest total of 10,456 individuals, shows a higher default rate of 9.3%. While the "Working" group has a higher default percentage, it contributes the most to the total number of default cases. Income types such as "Unemployed" and "Student" have extremely small counts and do not significantly impact the overall default cases.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Does Education Type Impact Credit Default Probability?

# COMMAND ----------

education_type = spark.sql("""
SELECT 
    NAME_EDUCATION_TYPE,
    SUM(CASE WHEN TARGET = 0 THEN 1 ELSE 0 END) AS target_0_count,
    SUM(CASE WHEN TARGET = 1 THEN 1 ELSE 0 END) AS target_1_count,
    COUNT(*) AS total_count,
    ROUND((SUM(CASE WHEN TARGET = 0 THEN 1 ELSE 0 END) / COUNT(*)) * 100, 2) AS target_0_percentage,
    ROUND((SUM(CASE WHEN TARGET = 1 THEN 1 ELSE 0 END) / COUNT(*)) * 100, 2) AS target_1_percentage
FROM credit_data
GROUP BY NAME_EDUCATION_TYPE
""")

education_type.show()


# COMMAND ----------

# MAGIC %md
# MAGIC The "Academic degree" education type shows no default cases (100% target 0), but the sample size is extremely small, with only 11 individuals. For other education types, "Higher education" has a relatively low default rate of 4.82%, with a total of 4,920 individuals, indicating that people with higher education are less likely to face payment issues. In contrast, "Incomplete higher" and "Lower secondary" education types show slightly higher default rates of 9.79% and 10.16%, though the sample sizes are smaller. The "Secondary / secondary special" group, with a large number of 14,149 individuals, has a default rate of 8.83%, meaning this group contributes the largest share of the total default cases.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Does Family Status Influence the Likelihood of Default?

# COMMAND ----------

family_status = spark.sql("""
SELECT 
    NAME_FAMILY_STATUS,
    SUM(CASE WHEN TARGET = 0 THEN 1 ELSE 0 END) AS target_0_count,
    SUM(CASE WHEN TARGET = 1 THEN 1 ELSE 0 END) AS target_1_count,
    COUNT(*) AS total_count,
    ROUND((SUM(CASE WHEN TARGET = 0 THEN 1 ELSE 0 END) / COUNT(*)) * 100, 2) AS target_0_percentage,
    ROUND((SUM(CASE WHEN TARGET = 1 THEN 1 ELSE 0 END) / COUNT(*)) * 100, 2) AS target_1_percentage
FROM credit_data
GROUP BY NAME_FAMILY_STATUS
""")

family_status.show()


# COMMAND ----------

# MAGIC %md
# MAGIC The "Widow" family status type has the lowest default rate at 5.87%, with a total of 1,039 individuals, indicating that widows are less likely to face payment issues. On the other hand, the "Single / not married" status shows a slightly higher default rate of 9.99%, with a total of 2,864 individuals. "Separated" and "Civil marriage" statuses have similar default rates at 8.68% and 9.37%, with total counts of 1,244 and 1,931, respectively. Meanwhile, "Married" status, despite having the highest total number of individuals (12,922), shows a relatively lower default rate of 7.29%, indicating that married individuals have a lower risk of default.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Does Housing Type Affect the Probability of Default?

# COMMAND ----------

housing_type = spark.sql("""
SELECT 
    NAME_HOUSING_TYPE,
    SUM(CASE WHEN TARGET = 0 THEN 1 ELSE 0 END) AS target_0_count,
    SUM(CASE WHEN TARGET = 1 THEN 1 ELSE 0 END) AS target_1_count,
    COUNT(*) AS total_count,
    ROUND((SUM(CASE WHEN TARGET = 0 THEN 1 ELSE 0 END) / COUNT(*)) * 100, 2) AS target_0_percentage,
    ROUND((SUM(CASE WHEN TARGET = 1 THEN 1 ELSE 0 END) / COUNT(*)) * 100, 2) AS target_1_percentage
FROM credit_data
GROUP BY NAME_HOUSING_TYPE
""")

housing_type.show()


# COMMAND ----------

# MAGIC %md
# MAGIC The "House / apartment" housing type has the largest number of individuals, with a total of 17,733 people and a default rate of 7.63%, indicating that although the total number is very high, the proportion of people facing default remains relatively low. "Municipal apartment" and "Co-op apartment" types show similar default rates of 7.88% and 7.5%, with significantly fewer individuals, totaling 749 and 80, respectively. "Rented apartment" and "With parents" housing types show higher default rates, at 11.8% and 11.67%, although with smaller total counts of 305 and 968 individuals. Overall, housing types such as house/apartment and co-op apartments tend to have lower default rates compared to rented or living with parents arrangements.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Does Occupation Type Influence the Risk of Default?

# COMMAND ----------

occupation_type = spark.sql("""
SELECT 
    OCCUPATION_TYPE,
    SUM(CASE WHEN TARGET = 0 THEN 1 ELSE 0 END) AS target_0_count,
    SUM(CASE WHEN TARGET = 1 THEN 1 ELSE 0 END) AS target_1_count,
    COUNT(*) AS total_count,
    ROUND((SUM(CASE WHEN TARGET = 0 THEN 1 ELSE 0 END) / COUNT(*)) * 100, 2) AS target_0_percentage,
    ROUND((SUM(CASE WHEN TARGET = 1 THEN 1 ELSE 0 END) / COUNT(*)) * 100, 2) AS target_1_percentage
FROM credit_data
GROUP BY OCCUPATION_TYPE
""")

occupation_type.show()


# COMMAND ----------

# MAGIC %md
# MAGIC Among the top 10 occupations, "Managers" has the highest number with 1,348 individuals and a default rate of 6.9%, which is relatively low. Occupations such as "HR staff", "Accountants", and "Medicine staff" show very low default rates, at 5.41%, 5.76%, and 7.12%, with total counts not being very large. "Laborers" have a large total count of 9,807 individuals, with a default rate of 7.39%, slightly higher than managers, but still relatively low. On the other hand, occupations like "Cleaning staff", "Drivers", and "Low-skill Laborers" show higher default rates, at 11.07%, 11.44%, and 17.5%, though these categories have smaller total numbers. Generally, high-skill jobs like managers and accountants tend to have lower default rates compared to low-skill or more manual labor occupations.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Does the Number of Family Members Impact Default Risk?

# COMMAND ----------

family_members = spark.sql("""
SELECT 
    CNT_FAM_MEMBERS,
    SUM(CASE WHEN TARGET = 0 THEN 1 ELSE 0 END) AS target_0_count,
    SUM(CASE WHEN TARGET = 1 THEN 1 ELSE 0 END) AS target_1_count,
    COUNT(*) AS total_count,
    ROUND((SUM(CASE WHEN TARGET = 0 THEN 1 ELSE 0 END) / COUNT(*)) * 100, 2) AS target_0_percentage,
    ROUND((SUM(CASE WHEN TARGET = 1 THEN 1 ELSE 0 END) / COUNT(*)) * 100, 2) AS target_1_percentage
FROM credit_data
GROUP BY CNT_FAM_MEMBERS
""")

family_members.show()


# COMMAND ----------

# MAGIC %md
# MAGIC Among the family size categories, the category with one family member ("1") has the highest total count (4,277 people), with a relatively low default rate of 8.53%. The category with two family members ("2") has a very large total count of 10,412 people, with an even lower default rate of 6.95%. Some categories with more family members, such as "6", "5", "4", and "3", show slightly higher default rates, with the "6" category having the highest default rate at 21.43%. Categories with more than six family members ("9", "8", "10") have very few individuals, with 100% default rate in categories "9", "8", and "10", although the total counts are very small. Overall, categories with smaller family sizes tend to have lower default rates compared to categories with larger family sizes.

# COMMAND ----------

# MAGIC %md
# MAGIC ## How Does Region Type Relate to Default Risk?

# COMMAND ----------

region_rating = spark.sql("""
SELECT 
    REGION_RATING_CLIENT,
    SUM(CASE WHEN TARGET = 0 THEN 1 ELSE 0 END) AS target_0_count,
    SUM(CASE WHEN TARGET = 1 THEN 1 ELSE 0 END) AS target_1_count,
    COUNT(*) AS total_count,
    ROUND((SUM(CASE WHEN TARGET = 0 THEN 1 ELSE 0 END) / COUNT(*)) * 100, 2) AS target_0_percentage,
    ROUND((SUM(CASE WHEN TARGET = 1 THEN 1 ELSE 0 END) / COUNT(*)) * 100, 2) AS target_1_percentage
FROM credit_data
GROUP BY REGION_RATING_CLIENT
""")

region_rating.show()


# COMMAND ----------

# MAGIC %md
# MAGIC The highest default rate is found in the "3" category, with a default rate of 10.83%. Although this category has a relatively large total (3,084 people), its default rate is still higher compared to categories "1" and "2". Category "1" with 2,115 people shows a very low default rate of just 4.16%. Meanwhile, category "2", with a total of 14,801 people, has a default rate of 7.81%, which is higher than category "1" but lower than category "3". Overall, regions with a lower rating tend to have a lower default rate.

# COMMAND ----------

# MAGIC %md
# MAGIC ## How Does Income Level Impact Default Risk?

# COMMAND ----------

# Calculate the minimum and maximum values of the AMT_INCOME_TOTAL column
income_minmax = spark.sql("""
SELECT 
    MIN(AMT_INCOME_TOTAL) AS min_income,
    MAX(AMT_INCOME_TOTAL) AS max_income
FROM credit_data
""")
income_minmax.show()


# COMMAND ----------

income_grouped = spark.sql("""
SELECT 
    CASE 
        WHEN AMT_INCOME_TOTAL < 50000 THEN 'Low Income'
        WHEN AMT_INCOME_TOTAL BETWEEN 50000 AND 100000 THEN 'Medium Income'
        WHEN AMT_INCOME_TOTAL BETWEEN 100000 AND 500000 THEN 'High Income'
        ELSE 'Very High Income'
    END AS income_category,
    SUM(CASE WHEN TARGET = 0 THEN 1 ELSE 0 END) AS target_0_count,
    SUM(CASE WHEN TARGET = 1 THEN 1 ELSE 0 END) AS target_1_count,
    COUNT(*) AS total_count,
    ROUND((SUM(CASE WHEN TARGET = 0 THEN 1 ELSE 0 END) / COUNT(*)) * 100, 2) AS target_0_percentage,
    ROUND((SUM(CASE WHEN TARGET = 1 THEN 1 ELSE 0 END) / COUNT(*)) * 100, 2) AS target_1_percentage
FROM credit_data
GROUP BY income_category
ORDER BY income_category
""")

income_grouped.show()


# COMMAND ----------

# MAGIC %md
# MAGIC From the available data, the "High Income" category has the largest number of clients, with over 15,000 individuals. Despite this large number, the default rate in this category remains relatively low at 7.98%. Followed by the "Medium Income" category, with a default rate of 7.58%, showing a similar resilience to default risk. The "Low Income" and "Very High Income" categories have slightly higher default rates, at 7.72% and 7.14% respectively, but the difference is minimal. Overall, all income categories show relatively low default rates, with little variation between them.

# COMMAND ----------

# MAGIC %md
# MAGIC ## How Does Total Credit Amount Affect Default Risk?

# COMMAND ----------

# Query to find the minimum and maximum values in the AMT_CREDIT column
amt_credit_minmax = spark.sql("""
SELECT 
    MIN(AMT_CREDIT) AS min_credit, 
    MAX(AMT_CREDIT) AS max_credit 
FROM credit_data
""")
amt_credit_minmax.show()


# COMMAND ----------

amt_credit_target = spark.sql("""
SELECT 
    CASE 
        WHEN AMT_CREDIT < 500000 THEN 'Low'
        WHEN AMT_CREDIT BETWEEN 500000 AND 1000000 THEN 'Medium'
        WHEN AMT_CREDIT BETWEEN 1000000 AND 2000000 THEN 'High'
        ELSE 'Very High'
    END AS credit_category,
    SUM(CASE WHEN TARGET = 0 THEN 1 ELSE 0 END) AS target_0_count,
    SUM(CASE WHEN TARGET = 1 THEN 1 ELSE 0 END) AS target_1_count,
    COUNT(*) AS total_count,
    ROUND((SUM(CASE WHEN TARGET = 0 THEN 1 ELSE 0 END) / COUNT(*)) * 100, 2) AS target_0_percentage,
    ROUND((SUM(CASE WHEN TARGET = 1 THEN 1 ELSE 0 END) / COUNT(*)) * 100, 2) AS target_1_percentage
FROM credit_data
GROUP BY credit_category
ORDER BY credit_category
""")

amt_credit_target.show()


# COMMAND ----------

# MAGIC %md
# MAGIC From the available data, the "Very High" category shows an extremely low default rate of just 1.56%, although the number of individuals in this category is relatively small. The "High" category has a slightly higher default rate at 5.72%, but it remains low compared to other categories. Meanwhile, the "Low" and "Medium" categories show slightly higher default rates, at 8.32% and 8.38%, respectively. Overall, higher credit categories (High and Very High) tend to have a lower default risk compared to lower credit categories.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Does Annuity Type Influence Default Risk?

# COMMAND ----------

min_max_annuity = spark.sql("""
SELECT 
    MIN(AMT_ANNUITY) AS min_annuity,
    MAX(AMT_ANNUITY) AS max_annuity
FROM credit_data
""")

min_max_annuity.show()


# COMMAND ----------

amt_annuity_target = spark.sql("""
SELECT 
    CASE 
        WHEN AMT_ANNUITY < 10000 THEN 'Low'
        WHEN AMT_ANNUITY >= 10000 AND AMT_ANNUITY < 50000 THEN 'Medium'
        WHEN AMT_ANNUITY >= 50000 AND AMT_ANNUITY < 150000 THEN 'High'
        ELSE 'Very High'
    END AS annuity_range,
    SUM(CASE WHEN TARGET = 0 THEN 1 ELSE 0 END) AS target_0_count,
    SUM(CASE WHEN TARGET = 1 THEN 1 ELSE 0 END) AS target_1_count,
    COUNT(*) AS total_count,
    ROUND((SUM(CASE WHEN TARGET = 0 THEN 1 ELSE 0 END) / COUNT(*)) * 100, 2) AS target_0_percentage,
    ROUND((SUM(CASE WHEN TARGET = 1 THEN 1 ELSE 0 END) / COUNT(*)) * 100, 2) AS target_1_percentage
FROM credit_data
GROUP BY annuity_range
ORDER BY annuity_range
""")

amt_annuity_target.show()


# COMMAND ----------

# MAGIC %md
# MAGIC The "Very High" category shows an extremely low default rate (0%), although the number of individuals is very small, with only 4 people. The "High" and "Low" categories have almost identical default rates, around 5.7%, with a slight difference between the two. The "Medium" category has a much larger number of individuals and a slightly higher default rate of 8.26%. Overall, higher annuity categories show lower default risk compared to lower annuity categories.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Does Perceived Good Price Affect Default Risk?

# COMMAND ----------

# Calculating min and max for the AMT_GOODS_PRICE column
min_max_goods_price = spark.sql("""
SELECT 
    MIN(AMT_GOODS_PRICE) AS min_goods_price, 
    MAX(AMT_GOODS_PRICE) AS max_goods_price
FROM credit_data
""")

min_max_goods_price.show()


# COMMAND ----------

goods_price_category = spark.sql("""
SELECT 
    CASE 
        WHEN AMT_GOODS_PRICE < 100000 THEN 'Low'
        WHEN AMT_GOODS_PRICE >= 100000 AND AMT_GOODS_PRICE < 500000 THEN 'Medium'
        WHEN AMT_GOODS_PRICE >= 500000 AND AMT_GOODS_PRICE < 1000000 THEN 'High'
        ELSE 'Very High'
    END AS goods_price_category,
    SUM(CASE WHEN TARGET = 0 THEN 1 ELSE 0 END) AS target_0_count,
    SUM(CASE WHEN TARGET = 1 THEN 1 ELSE 0 END) AS target_1_count,
    COUNT(*) AS total_count,
    ROUND((SUM(CASE WHEN TARGET = 0 THEN 1 ELSE 0 END) / COUNT(*)) * 100, 2) AS target_0_percentage,
    ROUND((SUM(CASE WHEN TARGET = 1 THEN 1 ELSE 0 END) / COUNT(*)) * 100, 2) AS target_1_percentage
FROM credit_data
GROUP BY goods_price_category
ORDER BY goods_price_category
""")

goods_price_category.show()


# COMMAND ----------

# MAGIC %md
# MAGIC The "Very High" category has the lowest default rate (4.53%) with a significant number of individuals (2,206). The "High" category has slightly more individuals (5,948) and a default rate of about 7.2%. The "Medium" category shows a higher default rate (9.03%), though it has the largest number of individuals (11,248). The "Low" category has a relatively low default rate (5.69%) despite the smaller number of individuals (598).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Does Age Influence Default Risk?

# COMMAND ----------

# Convert DAYS_BIRTH to years
credit_scoring_df = credit_scoring_df.withColumn(
    'AGE_YEARS',
    (F.col('DAYS_BIRTH') / -365).cast('int')
)

credit_scoring_df.select('DAYS_BIRTH', 'AGE_YEARS').show()


# COMMAND ----------

credit_scoring_df.createOrReplaceTempView("credit_data")

# COMMAND ----------

age_min_max = spark.sql("""
SELECT 
    MIN(AGE_YEARS) AS min_age,
    MAX(AGE_YEARS) AS max_age
FROM credit_data
""")

age_min_max.show()

# COMMAND ----------

age_category = spark.sql("""
SELECT 
    CASE 
        WHEN AGE_YEARS BETWEEN 21 AND 35 THEN 'Young Adult'
        WHEN AGE_YEARS BETWEEN 36 AND 55 THEN 'Adult'
        WHEN AGE_YEARS BETWEEN 56 AND 68 THEN 'Senior'
        ELSE 'Undefined' 
    END AS age_group,
    SUM(CASE WHEN TARGET = 0 THEN 1 ELSE 0 END) AS target_0_count,
    SUM(CASE WHEN TARGET = 1 THEN 1 ELSE 0 END) AS target_1_count,
    COUNT(*) AS total_count,
    ROUND((SUM(CASE WHEN TARGET = 0 THEN 1 ELSE 0 END) / COUNT(*)) * 100, 2) AS target_0_percentage,
    ROUND((SUM(CASE WHEN TARGET = 1 THEN 1 ELSE 0 END) / COUNT(*)) * 100, 2) AS target_1_percentage
FROM credit_data
GROUP BY age_group
ORDER BY age_group
""")

age_category.show()


# COMMAND ----------

# MAGIC %md
# MAGIC The adult group has the largest number, with a default rate of 7.29%, indicating financial stability due to being in a more established phase of life. The senior group shows an even lower default rate of 4.91%, as many individuals are retired and have fixed incomes. In contrast, the young adult group has the highest default rate at 10.79%, which is attributed to a lack of experience in financial management and a higher tendency to use credit more extensively.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Does Employment Tenure Affect Default Risk?

# COMMAND ----------

# Convert DAYS_EMPLOYED to years
credit_scoring_df = credit_scoring_df.withColumn(
    'YEARS_EMPLOYED',
    (F.col('DAYS_EMPLOYED') / -365).cast('int')
)

credit_scoring_df.select('DAYS_EMPLOYED', 'YEARS_EMPLOYED').show()


# COMMAND ----------

credit_scoring_df.createOrReplaceTempView("credit_data")

# COMMAND ----------

min_max_years_employed = spark.sql("""
SELECT 
    MIN(YEARS_EMPLOYED) AS min_years_employed,
    MAX(YEARS_EMPLOYED) AS max_years_employed
FROM credit_data
""") 

min_max_years_employed.show()


# COMMAND ----------

years_employed_categories = spark.sql("""
SELECT 
    CASE 
        WHEN YEARS_EMPLOYED = -1000 THEN 'Unknown'
        WHEN YEARS_EMPLOYED = 0 THEN 'Fresh'
        WHEN YEARS_EMPLOYED BETWEEN 1 AND 2 THEN 'Junior'
        WHEN YEARS_EMPLOYED BETWEEN 3 AND 5 THEN 'Mid-level'
        WHEN YEARS_EMPLOYED BETWEEN 6 AND 10 THEN 'Senior'
        ELSE 'Experienced'
    END AS employment_category,
    SUM(CASE WHEN TARGET = 0 THEN 1 ELSE 0 END) AS target_0_count,
    SUM(CASE WHEN TARGET = 1 THEN 1 ELSE 0 END) AS target_1_count,
    COUNT(*) AS total_count,
    ROUND((SUM(CASE WHEN TARGET = 0 THEN 1 ELSE 0 END) / COUNT(*)) * 100, 2) AS target_0_percentage,
    ROUND((SUM(CASE WHEN TARGET = 1 THEN 1 ELSE 0 END) / COUNT(*)) * 100, 2) AS target_1_percentage
FROM credit_data
GROUP BY employment_category
ORDER BY employment_category
""")

years_employed_categories.show()


# COMMAND ----------

# MAGIC %md
# MAGIC The "Experienced" group has the lowest default rate at 4.67%, reflecting better financial stability likely due to their experience in managing finances. In contrast, the "Fresh" group has a higher default rate of 10.61%, possibly due to financial uncertainty and lack of debt management experience. The "Junior" and "Mid-level" groups have similar default rates around 10%, suggesting that while they have work experience, they still face challenges in financial management, particularly in the early stages of their careers. The "Senior" group shows a slightly lower default rate at 7.34%, which could be attributed to more stable incomes and matured financial experience. Meanwhile, the "Unknown" category has a relatively low default rate of 5.32%, indicating that even though they are unclassified, they still show financial stability.

# COMMAND ----------


