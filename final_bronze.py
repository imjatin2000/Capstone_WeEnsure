# Databricks notebook source
display(dbutils.fs.ls("/mnt/batchdata/unzipped/"))

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
import dlt

# COMMAND ----------

@dlt.create_table(
  comment="The Raw Customers table",
  table_properties={
    "WeEnsure.quality": "bronze",
    "pipelines.autoOptimize.managed": "true"
  }
)
def customers_raw():
    """
    This function creates customers_raw delta live table by reading the customers data from the ADLS container. 

    Returns:
        None
    """
    customers_df = spark.read.option("multiline", "true").csv("dbfs:/mnt/batchdata/unzipped/customers.csv", header=True, inferSchema=True)
    return customers_df

# COMMAND ----------

@dlt.create_table(
  comment="The raw Agents table",
  table_properties={
    "WeEnsure.quality": "bronze",
    "pipelines.autoOptimize.managed": "true"
  }
)
def agents_raw():
    """
    This function creates agents_raw delta live table by reading the agents data from the ADLS container. 

    Returns:
        None
    """
    agents_df = spark.read.option("multiline", "true").csv("dbfs:/mnt/batchdata/unzipped/agents.csv", header=True, inferSchema=True)
    return agents_df

# COMMAND ----------

@dlt.create_table(
  comment="The Raw Payment Table",
  table_properties={
    "WeEnsure.quality": "bronze",
    "pipelines.autoOptimize.managed": "true"
  }
)
def payments_raw():
    """
    This function creates payments_raw delta live table by reading the payments data from the ADLS container. 

    Returns:
        None
    """
    payment_df = spark.read.csv("dbfs:/mnt/batchdata/unzipped/Payment.csv", header=True, inferSchema=True)
    return payment_df

# COMMAND ----------

@dlt.create_table(
  comment="The raw reimbursement",
  table_properties={
    "WeEnsure.quality": "bronze",
    "pipelines.autoOptimize.managed": "true"
  }
)
def reimbursement_raw():
    """
    This function creates reimbursement_raw delta live table by reading the reimbursement data from the ADLS container. 

    Returns:
        None
    """
    reimbursement_df = spark.read.csv("dbfs:/mnt/batchdata/unzipped/Reimbursement.csv", header=True, inferSchema=True)
    return reimbursement_df

# COMMAND ----------

@dlt.create_table(
  comment="The raw Providers",
  table_properties={
    "WeEnsure.quality": "bronze",
    "pipelines.autoOptimize.managed": "true"
  }
)
def provider_raw():
    """
    This function creates provider_raw delta live table by reading the provider data from the ADLS container. 

    Returns:
        None
    """
    provider_df = spark.read.option("multiline", "true").csv("dbfs:/mnt/batchdata/unzipped/Provider.csv", header=True, inferSchema=True)
    return provider_df

# COMMAND ----------

@dlt.create_table(
  comment="The raw Claims table",
  table_properties={
    "WeEnsure.quality": "bronze",
    "pipelines.autoOptimize.managed": "true"
  }
)
def claims_raw():
    """
    This function creates claims_raw delta live table by reading the claims data from the ADLS container. 

    Returns:
        None
    """
    claims_df = spark.read.csv("dbfs:/mnt/batchdata/unzipped/Claims.csv", header=True, inferSchema=True)
    return claims_df


# COMMAND ----------

@dlt.create_table(
  comment="The raw Plans table",
  table_properties={
    "WeEnsure.quality": "bronze",
    "pipelines.autoOptimize.managed": "true"
  }
)
def plans_raw():
    """
    This function creates plans_raw delta live table by reading the plans data from the ADLS container. 

    Returns:
        None
    """
    plans_df = spark.read.csv("dbfs:/mnt/batchdata/unzipped/Plans.csv", header=True, inferSchema=True)
    return plans_df

# COMMAND ----------

@dlt.create_table(
  comment="The raw Policies table",
  table_properties={
    "WeEnsure.quality": "bronze",
    "pipelines.autoOptimize.managed": "true"
  }
)
def policies_raw():
    """
    This function creates policies_raw delta live table by reading the policies data from the ADLS container. 

    Returns:
        None
    """
    policies_df = spark.read.csv("dbfs:/mnt/batchdata/unzipped/Policies.csv", header=True, inferSchema=True)
    return policies_df


# COMMAND ----------

@dlt.create_table(
  comment="The raw Rejected_claims table",
  table_properties={
    "WeEnsure.quality": "bronze",
    "pipelines.autoOptimize.managed": "true"
  }
)
def rejected_claims_raw():
    """
    This function creates rejected_claims_raw delta live table by reading the rejected_claims data from the ADLS container. 

    Returns:
        None
    """
    rejected_claims_df = spark.read.csv("dbfs:/mnt/batchdata/unzipped/Rejected_claims.csv", header=True, inferSchema=True)
    return rejected_claims_df

# COMMAND ----------

@dlt.create_table(
  comment="The raw Subscribers table",
  table_properties={
    "WeEnsure.quality": "bronze",
    "pipelines.autoOptimize.managed": "true"
  }
)
def subscribers_raw():
    """
    This function creates subscribers_raw delta live table by reading the subscribers data from the ADLS container. 

    Returns:
        None
    """
    subscribers_df = spark.read.option("multiline", "true").csv("dbfs:/mnt/batchdata/unzipped/subscribers.csv", header=True, inferSchema=True)
    return subscribers_df

# COMMAND ----------

# MAGIC %md
# MAGIC streaming

# COMMAND ----------

@dlt.create_table(
  comment="The raw customers stream data.",
  table_properties={
    "WeEnsure_delta.quality": "bronze",
    "pipelines.autoOptimize.managed": "true"
  }
)

def customer_stream_raw():
    """
    This function creates customers_stream_raw delta live table by reading the streamed customers data from the stremed ADLS container. This also alters the column table as a full string without whitespaces.

    Returns:
        None
    """
    stream_df = spark.readStream.format("cloudFiles") \
                          .option("cloudFiles.format", "parquet") \
                          .option("cloudFiles.schemaLocation", 
                                  "dbfs:/mnt/streaming/loader_stream/_schemas/") \
                          .load("dbfs:/mnt/weensurestreaming/")
    column_names = stream_df.columns
    for column_name in column_names:
        stream_df = stream_df.withColumnRenamed(column_name, column_name.replace(" ", "_"))
    return stream_df
