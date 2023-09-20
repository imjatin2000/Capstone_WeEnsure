# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
import dlt

# COMMAND ----------

# MAGIC %run "/Users/jatin_1692255857312@npmentorskool.onmicrosoft.com/capstone/bronze"

# COMMAND ----------

# MAGIC %md
# MAGIC # CUSTOMERS

# COMMAND ----------

@dlt.create_table(
  comment="This table contains cleaned customer data ingested from the Delta Lake, partitioned by agent_id.",
  partition_cols=["agent_id"],
  table_properties={
    "WeEnsure.quality": "silver",
    "pipelines.autoOptimize.managed": "true"
  }
)
@dlt.expect_all({"valid_customer": "customer_id IS NOT NULL ","valid_phone":"len(phone) == 10","valid_family":"family_members >= 0"})
def customers_clean():
    """
    Cleans and prepares customer data.

    Reads raw customer data from 'customers_raw', performs necessary cleaning steps, and returns the cleaned DataFrame.

    Returns:
        DataFrame: A DataFrame containing cleaned customer data.
    """
    customers_df = dlt.read('customers_raw')
    customers_df = customers_df.select([col(column).alias(column.lower()) for column in customers_df.columns])
    customers_df = customers_df.dropDuplicates()
    return customers_df


# COMMAND ----------

# MAGIC %md
# MAGIC # PAYMENTS

# COMMAND ----------

@dlt.create_table(
  comment="The cleaned payments, ingested from delta and partitioned by paid_amount",
  partition_cols=["paid_amount"],
  table_properties={
    "WeEnsure.quality": "silver",
    "pipelines.autoOptimize.managed": "true"
  }
)
@dlt.expect_all({"valid_payment": "payment_id IS NOT NULL ","valid_policy": "policy_number IS NOT NULL ","valid_paid_amount": "paid_amount >0"})
def payments_clean():
    """
    This function reads the 'payments_raw' Delta table, performs data cleaning, and creates a new Delta table with cleaned data.

    Returns:
        DataFrame: A DataFrame containing cleaned payment data.
    """
    payments_df = dlt.read('payments_raw')
    payments_df = payments_df.select([col(column).alias(column.lower()) for column in payments_df.columns])
    columns_to_process = payments_df.columns
    for column in columns_to_process:
        payments_df = payments_df.withColumn(column, when(col(column) == "?", None).otherwise(col(column)))
    return payments_df


# COMMAND ----------

# MAGIC %md
# MAGIC # PLANS

# COMMAND ----------

@dlt.create_table(
    comment="The cleaned plans and partitioned by payment_type",
    partition_cols=["payment_type"],
    table_properties={
        "WeEnsure.quality": "silver",
        "pipelines.autoOptimize.managed": "true"
    }
)
@dlt.expect_all({
    "valid_plan_id": "plan_id IS NOT NULL",
    "valid_deductible": "deductible > 0",
    "valid_coinsurance": "coinsurance > 0",
    "valid_out_of_pocket_maximum": "out_of_pocket_maximum > 0",
    "valid_daily_premium": "daily_premium > 0"
})
def plans_clean():
    """
    Cleans and preprocesses the 'plans_raw' DataFrame.

    Reads the 'plans_raw' DataFrame, converts column names to lowercase, removes duplicates,
    and returns the cleaned DataFrame.

    Returns:
        pandas.DataFrame: A cleaned and preprocessed DataFrame of insurance plans.
    """
    plans_df = dlt.read('plans_raw')
    plans_df = plans_df.select([col(column).alias(column.lower()) for column in plans_df.columns])
    plans_df = plans_df.dropDuplicates()
    return plans_df


# COMMAND ----------

# MAGIC %md
# MAGIC # POLICIES

# COMMAND ----------

@dlt.create_table(
    comment="The cleaned policies and partitioned by plan_id",
    partition_cols=["plan_id"],
    table_properties={
        "WeEnsure.quality": "silver",
        "pipelines.autoOptimize.managed": "true"
    }
)
@dlt.expect_all(
    {
        "valid_policy": "policy_number IS NOT NULL and policy_start_date <= policy_end_date",
        "valid_daily_premium": "daily_premium > 0",
        "valid_insurance_coverage": "insurance_coverage > 0"
    }
)
def policies_clean():
    """
    Cleans and processes the raw policies data.

    Reads the 'policies_raw' DataFrame, converts column names to lowercase, removes duplicates,
    and returns the cleaned DataFrame.

    Returns:
        pandas.DataFrame: The cleaned policies DataFrame.
    """
    policies_df = dlt.read('policies_raw')
    policies_df = policies_df.select([col(column).alias(column.lower()) for column in policies_df.columns])
    policies_df = policies_df.dropDuplicates()
    return policies_df


# COMMAND ----------

# MAGIC %md
# MAGIC # PROVIDER
# MAGIC

# COMMAND ----------

@dlt.create_table(
  comment="The cleaned provider data table.",
  table_properties={
    "WeEnsure.quality": "silver",
    "pipelines.autoOptimize.managed": "true"
  }
)
@dlt.expect_all({"valid_provider": "provider_id IS NOT NULL ","valid_phone":"len(phone) =10"})

def provider_clean():
    """
    Cleans and prepares the 'provider_raw' data for further processing.

    This function reads the 'provider_raw' data, converts column names to lowercase,
    removes duplicate rows, and applies data quality checks for 'provider_id' and 'phone'.

    Returns:
        DataFrame: A cleaned DataFrame containing provider data.
    """
    provider_df = dlt.read('provider_raw')
    provider_df = provider_df.select([col(column).alias(column.lower()) for column in provider_df.columns])
    provider_df = provider_df.dropDuplicates()
    return provider_df


# COMMAND ----------

# MAGIC %md
# MAGIC # REIMBURSEMENT

# COMMAND ----------

@dlt.create_table(
  comment="The cleaned reimbursement",
  table_properties={
    "WeEnsure.quality": "silver",
    "pipelines.autoOptimize.managed": "true"
  }
)
@dlt.expect_all({"valid_claim": "claim_number IS NOT NULL ","valid_amount_approved": "amount_approved >0"})
def reimbursement_clean():
    """
    Cleans the reimbursement DataFrame.

    Returns:
        DataFrame: The cleaned reimbursement DataFrame.
    """
    reimbursement_df = dlt.read('reimbursement_raw')
    reimbursement_df = reimbursement_df.select([col(column).alias(column.lower()) for column in reimbursement_df.columns])
    reimbursement_df = reimbursement_df.dropDuplicates()
    return reimbursement_df


# COMMAND ----------

# MAGIC %md
# MAGIC # SUBSCRIBERS

# COMMAND ----------

@dlt.create_view(
  comment="The cleaned subscribers"
)
def subscribers_clean_view():
    """
    Cleans and transforms the 'subscribers_raw' DataFrame to create a cleaned view of subscribers data.

    This function performs the following transformations:
    1. Converts all column names to lowercase.
    2. Replaces '-' with None in the 'mail' column.
    3. Removes spaces, hyphens, and parentheses from the 'phone' column.
    4. Removes '+91' prefix from the 'phone' column.
    5. Replaces commas with '/' in the 'phone' column.
    6. Inserts '022' prefix in the 'phone' column where it is missing.
    7. Removes duplicate rows.

    Returns:
    - subscribers_df: A cleaned DataFrame containing subscriber data.

    Note: This function assumes the existence of a 'subscribers_raw' DataFrame.
    """
    subscribers_df = dlt.read('subscribers_raw')
    subscribers_df = subscribers_df.select([col(column).alias(column.lower()) for column in subscribers_df.columns])
    subscribers_df=subscribers_df.withColumn("mail", when(col("mail") == "-", None).otherwise(col("mail")))
    subscribers_df = subscribers_df.withColumn("phone", regexp_replace(col("phone"), "[(\\s\\-\\)]", ""))
    subscribers_df = subscribers_df.withColumn("phone", regexp_replace(col("phone"), "\\+91\\s*", ""))
    subscribers_df = subscribers_df.withColumn("phone", regexp_replace(col("phone"), ",", "/"))
    subscribers_df = subscribers_df.withColumn("phone", regexp_replace(col("phone"), "(\\d{11})([^/])", "$1/$2"))
    subscribers_df = subscribers_df.withColumn("phone", regexp_replace(col("phone"), r"/(\d{8})/", r"/022$1/"))
    subscribers_df = subscribers_df.dropDuplicates()
    return subscribers_df


# COMMAND ----------

@dlt.create_table(
  comment="The cleaned subscribers and partitioned by sub_type",
    partition_cols=["sub_type"],
  table_properties={
    "WeEnsure.quality": "silver",
    "pipelines.autoOptimize.managed": "true"
  }
)
@dlt.expect_all({"valid_subscriber": "sub_id IS NOT NULL ", "valid_phone":"len(phone)=10"})

def subscribers_clean():
    """
    Clean and process subscriber data.

    This function reads data from the 'subscribers_clean_view' source and performs data cleaning
    and processing tasks. It also partitions the data by 'sub_type' and sets certain table properties.

    Args:
        None

    Returns:
        DataFrame: A DataFrame containing cleaned subscriber data.

    Raises:
        None

    
    DLT Expectations:
        - valid_subscriber: Checks if 'sub_id' is not NULL.
        - valid_phone: Checks if the 'phone' column has a length of 10.

    Example:
        subscribers_data = subscribers_clean()
        subscribers_data.show()
    """
    subscribers_df = dlt.read('subscribers_clean_view')
    subscribers_df = subscribers_df.select([col(column).alias(column.lower()) for column in subscribers_df.columns])
    return subscribers_df


# COMMAND ----------

# MAGIC %md
# MAGIC # REJECTED CLAIMS

# COMMAND ----------

@dlt.create_table(
  comment="Creates a table for cleaned rejected claims and partitions it by reason.",
  partition_cols=["reason"],
  table_properties={
    "WeEnsure.quality": "silver",
    "pipelines.autoOptimize.managed": "true"
  }
)
@dlt.expect_all({"valid_claim": "claim_number IS NOT NULL", "valid_rejection": "rejection_id IS NOT NULL"})
def rejected_claims_clean():
    """
    Cleans and processes rejected claims data.

    This function reads the 'rejected_claims_raw' DataFrame, performs cleaning operations,
    and returns the cleaned DataFrame. The resulting table is partitioned by the 'reason' column.

    Returns:
        DataFrame: A DataFrame containing cleaned rejected claims data.
    """
    rejected_claims_df = dlt.read('rejected_claims_raw')
    rejected_claims_df = rejected_claims_df.select([col(column).alias(column.lower()) for column in rejected_claims_df.columns])
    rejected_claims_df = rejected_claims_df.dropDuplicates()
    return rejected_claims_df


# COMMAND ----------

# MAGIC %md
# MAGIC # AGENTS

# COMMAND ----------

@dlt.create_table(
    comment="The cleaned agents",
    table_properties={
        "WeEnsure.quality": "silver",
        "pipelines.autoOptimize.managed": "true"
    }
)
@dlt.expect_all({"valid_agent": "agent_id IS NOT NULL ", "valid_phone": "len(phone)=10"})
def agents_clean():
    """
    Cleans and processes the 'agents_raw' DataFrame.

    This function performs the following tasks:
    1. Reads the 'agents_raw' DataFrame.
    2. Converts column names to lowercase.
    3. Removes duplicate rows.

    Returns:
    pandas.DataFrame: The cleaned agents DataFrame.
    """
    agents_df = dlt.read('agents_raw')
    agents_df = agents_df.select([col(column).alias(column.lower()) for column in agents_df.columns])
    agents_df = agents_df.dropDuplicates()
    return agents_df


# COMMAND ----------

# MAGIC %md
# MAGIC # CLAIMS

# COMMAND ----------

@dlt.create_table(
  comment="The cleaned claims and partitioned by provider_id",
    partition_cols=["provider_id"],
  table_properties={
    "WeEnsure.quality": "silver",
    "pipelines.autoOptimize.managed": "true"
  }
)
@dlt.expect_all({"valid_provider": "provider_id IS NOT NULL ","valid_policy": "policy_number IS NOT NULL ","valid_claim": "claim_number IS NOT NULL"
                 ,"valid_amount_claimed": "amount_claimed >0"})


def claims_clean():
    """
    Cleans and processes the claims data, partitioning it by provider_id.

    This function reads the raw claims data, applies cleaning operations, and
    partitions the data by provider_id. It also ensures that the data meets certain
    validity criteria.

    Returns:
        DataFrame: A cleaned and partitioned DataFrame containing claims data.

    Raises:
        None

    Example Usage:
        cleaned_data = claims_clean()
    """
    claims_df = dlt.read('claims_raw')
    claims_df = claims_df.select([col(column).alias(column.lower()) for column in claims_df.columns])
    claims_df = claims_df.dropDuplicates()
    return claims_df


# COMMAND ----------

# MAGIC %md
# MAGIC # STREAMING

# COMMAND ----------

@dlt.create_table(
    comment="This table contains cleaned customer data ingested from Delta Lake.",
    table_properties={
        "WeEnsure.quality": "silver",
        "pipelines.autoOptimize.managed": "true"
    }
)
@dlt.expect_all({
    "valid_customer": "Customer_ID is not NULL",
    "valid_heart_rate": "Heart_Rate > 0",
    "valid_steps": "Total_Steps >= 0",
    "valid_calories": "Calories >= 0"
})
def customer_stream_clean():
    """
    Cleans and processes customer data from a raw stream and returns a cleaned DataFrame.

    Returns:
        DataFrame: A DataFrame containing cleaned customer data.
    """
    customer_df = dlt.read_stream('customer_stream_raw')
    customer_df = customer_df.select([col(column).alias(column.lower()) for column in customer_df.columns])

    # customer_df.write.format('delta').mode("overwrite").save("/mnt/path")

    return customer_df

