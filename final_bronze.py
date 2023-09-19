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
    Load and create the Raw Customers Delta Lake table.

    This function reads data from a CSV file, processes it, and stores it in a Delta Lake table.
    The table represents the raw, unprocessed data of customers.

    Returns:
        pyspark.sql.DataFrame: A Spark SQL DataFrame containing the raw customer data.
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
    Load and create the Raw Agents Delta Lake table.

    This function reads data from a CSV file, processes it, and stores it in a Delta Lake table.
    The table represents the raw, unprocessed data of agents.

    Returns:
        pyspark.sql.DataFrame: A Spark SQL DataFrame containing the raw agents data.
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
    Load and create the Raw Payments Delta Lake table.

    This function reads data from a CSV file, processes it, and stores it in a Delta Lake table.
    The table represents the raw, unprocessed data of payments.

    Returns:
        pyspark.sql.DataFrame: A Spark SQL DataFrame containing the raw payments data.
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
    Load and create the Raw Reimbursements Delta Lake table.

    This function reads data from a CSV file, processes it, and stores it in a Delta Lake table.
    The table represents the raw, unprocessed data of reimbursements.

    Returns:
        pyspark.sql.DataFrame: A Spark SQL DataFrame containing the raw reimbursements data.
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
    Load and create the Raw Providers Delta Lake table.

    This function reads data from a CSV file, processes it, and stores it in a Delta Lake table.
    The table represents the raw, unprocessed data of provider.

    Returns:
        pyspark.sql.DataFrame: A Spark SQL DataFrame containing the raw provider data.
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
    Load and create the Raw Claims Delta Lake table.

    This function reads data from a CSV file, processes it, and stores it in a Delta Lake table.
    The table represents the raw, unprocessed data of claims.

    Returns:
        pyspark.sql.DataFrame: A Spark SQL DataFrame containing the raw claims data.
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
    Load and create the Raw Plans Delta Lake table.

    This function reads data from a CSV file, processes it, and stores it in a Delta Lake table.
    The table represents the raw, unprocessed data of plans.

    Returns:
        pyspark.sql.DataFrame: A Spark SQL DataFrame containing the raw plans data.
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
    Load and create the Raw Policies Delta Lake table.

    This function reads data from a CSV file, processes it, and stores it in a Delta Lake table.
    The table represents the raw, unprocessed data of policies.

    Returns:
        pyspark.sql.DataFrame: A Spark SQL DataFrame containing the raw policies data.
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
    Load and create the Raw Rejected Claims Delta Lake table.

    This function reads data from a CSV file, processes it, and stores it in a Delta Lake table.
    The table represents the raw, unprocessed data of rejected claims.

    Returns:
        pyspark.sql.DataFrame: A Spark SQL DataFrame containing the raw rejected claims data.
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
    Load and create the Raw Subscribers Delta Lake table.

    This function reads data from a CSV file, processes it, and stores it in a Delta Lake table.
    The table represents the raw, unprocessed data of subscribers.

    Returns:
        pyspark.sql.DataFrame: A Spark SQL DataFrame containing the raw subscribers data.
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
    Load and create a Delta Lake table for raw customer streaming data.

    This function reads streaming data in Parquet format from a specified location
    and creates a Delta Lake table for the raw customer streaming data.

    Returns:
        pyspark.sql.DataFrame: A Spark SQL DataFrame containing the raw customer streaming data.
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
