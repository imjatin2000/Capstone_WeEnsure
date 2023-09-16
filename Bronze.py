# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
import dlt

# COMMAND ----------

display(dbutils.fs.ls("/mnt/weEnsures/unzipped"))

# COMMAND ----------

@dlt.create_table(
  comment="The reimbursement",
  table_properties={
    "Globalmart_delta.quality": "bronze",
    "pipelines.autoOptimize.managed": "true"
  }
)
def reimbursement():

    reimbursement_df = spark.read.csv("dbfs:/mnt/weEnsures/unzipped/Reimbursement.csv", header=True, inferSchema=True)
    return reimbursement_df

# COMMAND ----------

@dlt.create_table(
  comment="The Providers",
  table_properties={
    "Globalmart_delta.quality": "bronze",
    "pipelines.autoOptimize.managed": "true"
  }
)
def provider():

    provider_df = spark.read.csv("dbfs:/mnt/weEnsures/unzipped/Provider.csv", header=True, inferSchema=True)
    return provider_df

# COMMAND ----------

@dlt.create_table(
  comment="The Claims table",
  table_properties={
    "Globalmart_delta.quality": "bronze",
    "pipelines.autoOptimize.managed": "true"
  }
)
def claims():

    claims_df = spark.read.csv("dbfs:/mnt/weEnsures/unzipped/Claims.csv", header=True, inferSchema=True)
    return claims_df

# COMMAND ----------

@dlt.create_table(
  comment="The Payment Table",
  table_properties={
    "Globalmart_delta.quality": "bronze",
    "pipelines.autoOptimize.managed": "true"
  }
)
def payment():

    payment_df = spark.read.csv("dbfs:/mnt/weEnsures/unzipped/Payment.csv", header=True, inferSchema=True)
    return payment_df

# COMMAND ----------

@dlt.create_table(
  comment="The Plans table",
  table_properties={
    "Globalmart_delta.quality": "bronze",
    "pipelines.autoOptimize.managed": "true"
  }
)
def plans():

    plans_df = spark.read.csv("dbfs:/mnt/weEnsures/unzipped/Plans.csv", header=True, inferSchema=True)
    return plans_df

# COMMAND ----------

@dlt.create_table(
  comment="The Policies table",
  table_properties={
    "Globalmart_delta.quality": "bronze",
    "pipelines.autoOptimize.managed": "true"
  }
)
def policies():

    policies_df = spark.read.csv("dbfs:/mnt/weEnsures/unzipped/Policies.csv", header=True, inferSchema=True)
    return policies_df

# COMMAND ----------

@dlt.create_table(
  comment="The Rejected_claims table",
  table_properties={
    "Globalmart_delta.quality": "bronze",
    "pipelines.autoOptimize.managed": "true"
  }
)
def rejected_claims():

    rejected_claims_df = spark.read.csv("dbfs:/mnt/weEnsures/unzipped/Rejected_claims.csv", header=True, inferSchema=True)
    return rejected_claims_df

# COMMAND ----------

@dlt.create_table(
  comment="The Agents table",
  table_properties={
    "Globalmart_delta.quality": "bronze",
    "pipelines.autoOptimize.managed": "true"
  }
)
def agents():

    agents_df = spark.read.csv("dbfs:/mnt/weEnsures/unzipped/agents.csv", header=True, inferSchema=True)
    return agents_df

# COMMAND ----------

@dlt.create_table(
  comment="The Customers table",
  table_properties={
    "Globalmart_delta.quality": "bronze",
    "pipelines.autoOptimize.managed": "true"
  }
)
def customers():

    customers_df = spark.read.csv("dbfs:/mnt/weEnsures/unzipped/customers.csv", header=True, inferSchema=True)
    return customers_df

# COMMAND ----------

@dlt.create_table(
  comment="The Subscribers table",
  table_properties={
    "Globalmart_delta.quality": "bronze",
    "pipelines.autoOptimize.managed": "true"
  }
)
def subscribers():

    subscribers_df = spark.read.csv("dbfs:/mnt/weEnsures/unzipped/subscribers.csv", header=True, inferSchema=True)
    return subscribers_df
