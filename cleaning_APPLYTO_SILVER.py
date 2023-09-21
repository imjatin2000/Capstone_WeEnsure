# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *


# COMMAND ----------

customers=spark.read.table("capstone.customers_raw")
agents=spark.read.table("capstone.agents_raw")
claims=spark.read.table("capstone.claims_raw")
policies=spark.read.table("capstone.policies_raw")
provider=spark.read.table("capstone.provider_raw")
rejected_claims=spark.read.table("capstone.rejected_claims_raw")
subscribers=spark.read.table("capstone.subscribers_raw")
plans=spark.read.table("capstone.plans_raw")
reimbursement=spark.read.table("capstone.reimbursement_raw")
payments=spark.read.table("capstone.payments_raw")

# COMMAND ----------

dataframes=[customers,agents,claims,policies,provider,rejected_claims,subscribers,reimbursement,payments,plans]

# COMMAND ----------

# MAGIC %md
# MAGIC #### WeEnsure wants to ensure data consistency across various datasets by cross-referencing the distinct count with the total count of each dataset. Write a function to cross-check this.

# COMMAND ----------

for df in dataframes:
    print(f"dataframe:{df}")

    #calculate distinct count and total count
    distinct_count=df.distinct().count()
    total_count=df.count()

    print(f"distinct count: {distinct_count}")
    print(f"total count:{total_count}")

    #check if the count match
    if distinct_count==total_count:
         print("distinct count matches total count.\n")
    else:
         print("distinct count not matches total count.\n")

# COMMAND ----------

# MAGIC %md
# MAGIC #### only in policies dataframe distinct count not matches total count.lets see duplicate records
# MAGIC

# COMMAND ----------

duplicate_rows = policies.groupBy(*policies.columns).count().where(col("count") > 1)

# Show the duplicate rows
display(duplicate_rows)

# COMMAND ----------

policies = policies.dropDuplicates()

# COMMAND ----------

# MAGIC %md
# MAGIC #### now check for null values or ["", "na", "n/a", "N/A", "NA", "?"] in dataframes

# COMMAND ----------

dataframes=[customers,agents,claims,policies,provider,rejected_claims,subscribers,reimbursement,payments]
from pyspark.sql.functions import count, when, col

# Define a list of values to treat as null
null_values = ["", "na", "n/a", "N/A", "NA", "?"]

# Iterate through each DataFrame
for df in dataframes:
    print(f"Missing values count for DataFrame: {df}")
    
    # Create a new DataFrame to hold the counts
    missing_counts = df.select([count(when(col(c).isNull() | col(c).isin(null_values), c)).alias(c) for c in df.columns])
    missing_counts.show()


# COMMAND ----------

# MAGIC %md
# MAGIC #### there are null values in payment column

# COMMAND ----------

# MAGIC %md
# MAGIC # cleaning tables one by one

# COMMAND ----------

# MAGIC %md
# MAGIC # (1)CUSTOMERS
# MAGIC

# COMMAND ----------

customers.dtypes

# COMMAND ----------

def show_unique_values(dataframe: DataFrame, columns: list):
    """
    Display unique values in specified columns of a DataFrame.

    Args:
        dataframe (DataFrame): The DataFrame containing the data.
        columns (list): A list of column names for which unique values should be displayed.

    Returns:
        None

    This function takes a DataFrame and a list of column names, and for each column in the list,
    it computes and displays the unique values present in that column.
    """
    for column in columns:
        unique_values = dataframe.select(column).distinct()
        print(f"Unique values in {column}:")
        unique_values.show()

# COMMAND ----------

show_unique_values(customers, ["sex", "family_members"])

# COMMAND ----------

import re
import re

def extract_domain(email):
    """
    Extracts the domain from a given email address.

    Args:
        email (str): The email address from which to extract the domain.

    Returns:
        str: The extracted domain if a valid email address is provided, or "Invalid Email" if the email is invalid.

    Example:
        >>> extract_domain("user@example.com")
        'example.com'

        >>> extract_domain("invalid-email")
        'Invalid Email'
    """
    # Use a regular expression to extract the domain
    match = re.search(r"@([\w.-]+)", email)
    if match:
        return match.group(1)
    else:
        return "Invalid Email"

# Register the function as a UDF (User-Defined Function)
extract_domain_udf = udf(extract_domain, StringType())

# COMMAND ----------


# Apply the UDF to the DataFrame to create a new column "domain"
df = customers.withColumn("domain", extract_domain_udf(customers["customers_email"]))

# Get unique domain values
unique_domains = df.select("domain").distinct()
unique_domains.show()

# Filter and display invalid email addresses
invalid_emails = df.filter(df["domain"] == "Invalid Email").select("customers_email")
invalid_emails.show()

# COMMAND ----------

def is_valid_date(date_col):
    """
    Check if a date column is valid based on the 'yyyy-MM-dd' format.

    Parameters:
    date_col (pyspark.sql.Column): A PySpark DataFrame column representing dates in the 'yyyy-MM-dd' format.

    Returns:
    bool: True if the date column is valid, False otherwise.

    Example:
    >>> df = spark.createDataFrame([("2023-09-19",), ("2023-09-20",), ("2023-09-21",)], ["date"])
    >>> is_valid_date(df["date"])
    True
    """
    try:
        date_format = "yyyy-MM-dd"
        return date_col.isNotNull()
    except ValueError:
        return False

# COMMAND ----------

# Add a new column "date_validation" to the DataFrame
df = customers.withColumn("date_validation", is_valid_date(col("birthdate")))

# Show only rows with invalid dates
invalid_dates_df = df.filter(df["date_validation"] == False)
invalid_dates_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC #### every column in customers dataframe is correct and also there is no null values

# COMMAND ----------

# MAGIC %md
# MAGIC # (2)PAYMENTS

# COMMAND ----------

def find_rows_with_missing_values(dataframe: DataFrame):
    """
    Find rows with missing values in a DataFrame.

    Args:
        dataframe (DataFrame): The input DataFrame to search for rows with missing values.

    Returns:
        DataFrame: A new DataFrame containing rows with missing values.

    This function iterates through each row in the input DataFrame and checks each column for
    missing values. It considers various patterns such as "null," "n/a," "na," "N/A," "NA," and "?"
    as missing values. Rows with at least one missing value are added to the result DataFrame.
    The result DataFrame is returned and also displayed using the `show()` method.

    Example:
    >>> from pyspark.sql import SparkSession
    >>> spark = SparkSession.builder.appName("example").getOrCreate()
    >>> data = [(1, "John", None), (2, "Alice", "N/A"), (3, "Bob", "hello")]
    >>> columns = ["id", "name", "value"]
    >>> df = spark.createDataFrame(data, columns)
    >>> result = find_rows_with_missing_values(df)
    >>> result.show()
    +---+-----+-----+
    | id| name|value|
    +---+-----+-----+
    |  1| John| null|
    |  2|Alice|  N/A|
    +---+-----+-----+
    """
    
    # Define the list of null or missing value patterns to check
    missing_value_patterns = ["null", "n/a", "na", "N/A", "NA", "?"]
    
    # Initialize a list to store rows with missing values
    rows_with_missing_values = []
    
    # Iterate through each row in the DataFrame
    for row in dataframe.rdd.collect():
        # Check each column for missing values
        for column in dataframe.columns:
            cell_value = str(row[column]).strip().lower()
            if cell_value in missing_value_patterns:
                rows_with_missing_values.append(row)
                break  # Break the inner loop if any missing value is found in the row
    
    # Create a new DataFrame from the list of rows with missing values
    result_dataframe = dataframe.sparkSession.createDataFrame(rows_with_missing_values, dataframe.schema)
    
    return result_dataframe.show()


# COMMAND ----------

find_rows_with_missing_values(payments)

# COMMAND ----------

# MAGIC %md
# MAGIC there are 5 fields with ? sign we can replace this with none for future requirements . 

# COMMAND ----------

columns_to_process = payments.columns
for column in columns_to_process:
    df = payments.withColumn(column, when(col(column) == "?", None).otherwise(col(column)))

# COMMAND ----------

# MAGIC %md
# MAGIC # (3) PLANS

# COMMAND ----------

display(plans)

# COMMAND ----------

show_unique_values(plans, ['plan_id',
 'inpatient_care',
 'outpatient_care',
 'prescription_drugs',
 'mental_health_care',
 'dental_care',
 'vision_care',
 'deductible',
 'coinsurance',
 'out_of_pocket_maximum',
 'daily_premium',
 'payment_type'])

# COMMAND ----------

# MAGIC %md
# MAGIC all columns have right values in it

# COMMAND ----------

# MAGIC %md
# MAGIC # (4) POLICIES

# COMMAND ----------

# MAGIC %md
# MAGIC as we seen there are 4 duplicates in policies dataframe , remove it
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

display(policies)

# COMMAND ----------

def is_valid_date(date_col):
    """
    Check if a date column is valid based on the 'yyyy-MM-dd' format.

    Parameters:
    date_col (pyspark.sql.Column): A PySpark DataFrame column representing dates in the 'yyyy-MM-dd' format.

    Returns:
    bool: True if the date column is valid, False otherwise.

    Example:
    >>> df = spark.createDataFrame([("2023-09-19",), ("2023-09-20",), ("2023-09-21",)], ["date"])
    >>> is_valid_date(df["date"])
    True
    """
    
    try:
        date_format = "yyyy-MM-dd"
        return date_col.isNotNull()
    except ValueError:
        return False

# Add a new column "policy_start_date_validation" for policy_start_date
df = policies.withColumn("policy_start_date_validation", is_valid_date(col("policy_start_date")))
# Add a new column "end_date_validation" for end_date
df = df.withColumn("end_date_validation", is_valid_date(col("policy_end_date")))

# Show only rows with invalid dates in both columns
invalid_dates_df = df.filter((col("policy_start_date_validation") == False) | (col("end_date_validation") == False))
invalid_dates_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC every column is correct only need for drop duplicates

# COMMAND ----------

# MAGIC %md
# MAGIC #  (5) PROVIDER

# COMMAND ----------

display(provider)

# COMMAND ----------

# Filter rows with phone number length not equal to 10
provider_df= provider.filter(length(col("phone")) != 10 )
display(provider_df)

# COMMAND ----------

# MAGIC %md
# MAGIC there are 10 numbers of lenght 9 and rest are of length 10, we can adjust it as per our business requirement

# COMMAND ----------

# MAGIC %md
# MAGIC # (6) REIMBURSEMENT

# COMMAND ----------

display(reimbursement)

# COMMAND ----------

# Check if values in the "value" column are less than 0
filtered_df = reimbursement.filter(col("amount_approved") < 0)

# Show the DataFrame with values less than 0
filtered_df.show()

# COMMAND ----------

def is_valid_date(date_col):
    """
    Check if a date column is valid based on the 'yyyy-MM-dd' format.

    Parameters:
    date_col (pyspark.sql.Column): A PySpark DataFrame column representing dates in the 'yyyy-MM-dd' format.

    Returns:
    bool: True if the date column is valid, False otherwise.

    Example:
    >>> df = spark.createDataFrame([("2023-09-19",), ("2023-09-20",), ("2023-09-21",)], ["date"])
    >>> is_valid_date(df["date"])
    True
    """
    
    try:
        date_format = "yyyy-MM-dd"
        return date_col.isNotNull()
    except ValueError:
        return False

# Add a new column "date_validation" to the DataFrame
df = reimbursement.withColumn("date_validation", is_valid_date(col("reimbursement_date")))

# Show only rows with invalid dates
invalid_dates_df = df.filter(df["date_validation"] == False)
invalid_dates_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC all columns are correct and dont contain any null values

# COMMAND ----------

# MAGIC %md
# MAGIC # (7) SUBSCRIBERS

# COMMAND ----------

display(subscribers)

# COMMAND ----------

dataframes=[subscribers]

null_values = ["", "na", "n/a", "N/A", "NA", "?",'-']

# Iterate through each DataFrame
for df in dataframes:
    print(f"Missing values count for DataFrame: {df}")
    
    # Create a new DataFrame to hold the counts
    missing_counts = df.select([count(when(col(c).isNull() | col(c).isin(null_values), c)).alias(c) for c in df.columns])
    missing_counts.show()

# COMMAND ----------

# MAGIC %md 
# MAGIC there is one missing values in email replace it with null
# MAGIC

# COMMAND ----------

df=subscribers

# COMMAND ----------

df = df.withColumn("mail", when(col("mail") == "-", None).otherwise(col("mail")))

# COMMAND ----------

display(df)

# COMMAND ----------

def standardize_phone_numbers(column):

        # Remove commas, spaces, and dashes
        cleaned_phone = regexp_replace(column,  "[(\\s\\-\\)]", "")
        cleaned_phone = regexp_replace(cleaned_phone, "\\+91\\s*", "")
        cleaned_phone = regexp_replace(cleaned_phone,  ",", "/")
        standardized_phone = regexp_replace(cleaned_phone, "(\\d{11})([^/])", "$1/$2")

        return standardized_phone

# COMMAND ----------

dfs = df.withColumn("phone", standardize_phone_numbers(col("phone")))
dfs = dfs.withColumn("phone", regexp_replace(col("phone"), r"/(\d{8})/", r"/022$1/"))

# COMMAND ----------

display(dfs)

# COMMAND ----------

# MAGIC %md
# MAGIC this will make phone number consistent and now dataset is clean

# COMMAND ----------

# MAGIC %md
# MAGIC # (8) REJECTED CLAIMS

# COMMAND ----------

display(rejected_claims)

# COMMAND ----------


show_unique_values(rejected_claims, ["reason"])

# COMMAND ----------

def is_valid_date(date_col):
    """
    Check if a date column is valid based on the 'yyyy-MM-dd' format.

    Parameters:
    date_col (pyspark.sql.Column): A PySpark DataFrame column representing dates in the 'yyyy-MM-dd' format.

    Returns:
    bool: True if the date column is valid, False otherwise.

    Example:
    >>> df = spark.createDataFrame([("2023-09-19",), ("2023-09-20",), ("2023-09-21",)], ["date"])
    >>> is_valid_date(df["date"])
    True
    """
    
    try:
        date_format = "yyyy-MM-dd"
        return date_col.isNotNull()
    except ValueError:
        return False

# Add a new column "date_validation" to the DataFrame
df = rejected_claims.withColumn("date_validation", is_valid_date(col("rejection_date")))

# Show only rows with invalid dates
invalid_dates_df = df.filter(df["date_validation"] == False)
invalid_dates_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC everything fine

# COMMAND ----------

# MAGIC %md
# MAGIC # (9) AGENTS

# COMMAND ----------

display(agents)

# COMMAND ----------

#check for mail domains
df = agents.withColumn("domain", extract_domain_udf(agents["agent_email"]))

# Get unique domain values
unique_domains = df.select("domain").distinct()
unique_domains.show()

# Filter and display invalid email addresses
invalid_emails = df.filter(df["domain"] == "Invalid Email").select("agent_email")
invalid_emails.show()

# COMMAND ----------

# Extract the unique lengths of phone numbers and count their occurrences
phone_lengths_df = agents.withColumn("phone_length", length(col("phone"))) \
                     .groupBy("phone_length") \
                     .count() \
                     .orderBy("phone_length")

# Show the DataFrame with unique phone number lengths and their counts
phone_lengths_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC there are number of multiple lenght which we can change as per our business requirement
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # (10) CLAIMS

# COMMAND ----------

display(claims)

# COMMAND ----------

def is_valid_date(date_col):
    """
    Check if a date column is valid based on the 'yyyy-MM-dd' format.

    Parameters:
    date_col (pyspark.sql.Column): A PySpark DataFrame column representing dates in the 'yyyy-MM-dd' format.

    Returns:
    bool: True if the date column is valid, False otherwise.

    Example:
    >>> df = spark.createDataFrame([("2023-09-19",), ("2023-09-20",), ("2023-09-21",)], ["date"])
    >>> is_valid_date(df["date"])
    True
    """
    
    try:
        date_format = "yyyy-MM-dd"
        return date_col.isNotNull()
    except ValueError:
        return False

# Add a new column "date_validation" to the DataFrame
df = claims.withColumn("date_validation", is_valid_date(col("claim_date")))

# Show only rows with invalid dates
invalid_dates_df = df.filter(df["date_validation"] == False)
invalid_dates_df.show()

# COMMAND ----------


show_unique_values(claims, ["treatment"])

# COMMAND ----------

# Check if values in the "value" column are less than 0
filtered_df = claims.filter(col("amount_claimed") < 0)

# Show the DataFrame with values less than 0
filtered_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC everything looks fine

# COMMAND ----------

display(agents)

# COMMAND ----------


