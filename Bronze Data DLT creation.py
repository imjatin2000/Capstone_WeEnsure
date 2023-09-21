# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
import dlt

# COMMAND ----------

display(dbutils.fs.ls("/mnt/weEnsures/unzipped"))

# COMMAND ----------

@dlt.create_table(
  comment="The Rejected_claims table",
  table_properties={
    "Globalmart_delta.quality": "bronze",
    "pipelines.autoOptimize.managed": "true"
  }
)
def rejected_claims_raw():

    rejected_claims_df = spark.read.csv("dbfs:/mnt/weEnsures/unzipped/Rejected_claims.csv", header=True, inferSchema=True)
    return rejected_claims_df

# COMMAND ----------

@dlt.create_table(
  comment="The Subscribers table",
  table_properties={
    "Globalmart_delta.quality": "bronze",
    "pipelines.autoOptimize.managed": "true"
  }
)
def subscribers_raw():

    subscribers_df = spark.read.option('multiline','True').csv("dbfs:/mnt/weEnsures/unzipped/subscribers.csv", header=True, inferSchema=True)
    display(subscribers_df)
    return subscribers_df

# COMMAND ----------


