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
    "weEnsure_delta.quality": "bronze",
    "pipelines.autoOptimize.managed": "true"
  }
)
def customers_raw():

    customers_df = spark.read.option("multiline", "true").csv("dbfs:/mnt/batchdata/unzipped/customers.csv", header=True, inferSchema=True)
    return customers_df

# COMMAND ----------

@dlt.create_table(
  comment="The raw Agents table",
  table_properties={
    "weEnsure_delta.quality": "bronze",
    "pipelines.autoOptimize.managed": "true"
  }
)
def agents_raw():

    agents_df = spark.read.option("multiline", "true").csv("dbfs:/mnt/batchdata/unzipped/agents.csv", header=True, inferSchema=True)
    return agents_df

# COMMAND ----------

@dlt.create_table(
  comment="The Raw Payment Table",
  table_properties={
    "weEnsure_delta.quality": "bronze",
    "pipelines.autoOptimize.managed": "true"
  }
)
def payments_raw():
    payment_df = spark.read.csv("dbfs:/mnt/batchdata/unzipped/Payment.csv", header=True, inferSchema=True)
    return payment_df

# COMMAND ----------

@dlt.create_table(
  comment="The raw reimbursement",
  table_properties={
    "weEnsure_delta.quality": "bronze",
    "pipelines.autoOptimize.managed": "true"
  }
)
def reimbursement_raw():

    reimbursement_df = spark.read.csv("dbfs:/mnt/batchdata/unzipped/Reimbursement.csv", header=True, inferSchema=True)
    return reimbursement_df

# COMMAND ----------

@dlt.create_table(
  comment="The raw Providers",
  table_properties={
    "weEnsure_delta.quality": "bronze",
    "pipelines.autoOptimize.managed": "true"
  }
)
def provider_raw():

    provider_df = spark.read.option("multiline", "true").csv("dbfs:/mnt/batchdata/unzipped/Provider.csv", header=True, inferSchema=True)
    return provider_df

# COMMAND ----------

@dlt.create_table(
  comment="The raw Claims table",
  table_properties={
    "weEnsure_delta.quality": "bronze",
    "pipelines.autoOptimize.managed": "true"
  }
)
def claims_raw():

    claims_df = spark.read.csv("dbfs:/mnt/batchdata/unzipped/Claims.csv", header=True, inferSchema=True)
    return claims_df


# COMMAND ----------

@dlt.create_table(
  comment="The raw Plans table",
  table_properties={
    "weEnsure_delta.quality": "bronze",
    "pipelines.autoOptimize.managed": "true"
  }
)
def plans_raw():

    plans_df = spark.read.csv("dbfs:/mnt/batchdata/unzipped/Plans.csv", header=True, inferSchema=True)
    return plans_df

# COMMAND ----------

@dlt.create_table(
  comment="The raw Policies table",
  table_properties={
    "weEnsure_delta.quality": "bronze",
    "pipelines.autoOptimize.managed": "true"
  }
)
def policies_raw():
    policies_df = spark.read.csv("dbfs:/mnt/batchdata/unzipped/Policies.csv", header=True, inferSchema=True)
    return policies_df


# COMMAND ----------

@dlt.create_table(
  comment="The raw Rejected_claims table",
  table_properties={
    "weEnsure_delta.quality": "bronze",
    "pipelines.autoOptimize.managed": "true"
  }
)
def rejected_claims_raw():

    rejected_claims_df = spark.read.csv("dbfs:/mnt/batchdata/unzipped/Rejected_claims.csv", header=True, inferSchema=True)
    return rejected_claims_df

# COMMAND ----------

@dlt.create_table(
  comment="The raw Subscribers table",
  table_properties={
    "weEnsure_delta.quality": "bronze",
    "pipelines.autoOptimize.managed": "true"
  }
)
def subscribers_raw():
    subscribers_df = spark.read.option("multiline", "true").csv("dbfs:/mnt/batchdata/unzipped/subscribers.csv", header=True, inferSchema=True)
    return subscribers_df

# COMMAND ----------


