# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
import dlt

# COMMAND ----------

# MAGIC %run "/Repos/Capstone/Capstone_WeEnsure/Bronze Data DLT creation"

# COMMAND ----------

@dlt.create_table(
  comment="The cleaned rejected_claims and partitioned by reason",
    partition_cols=["reason"],
  table_properties={
    "Globalmart_deltaliv.quality": "silver",
    "pipelines.autoOptimize.managed": "true"
  }
)
@dlt.expect_all({"valid_claim": "claim_number IS NOT NULL "})

def rejected_claims_clean():
    rejected_claims_df = dlt.read('rejected_claims_raw')
    rejected_claims_df = rejected_claims_df.select([col(column).alias(column.lower()) for column in rejected_claims_df.columns])
    return rejected_claims_df

# COMMAND ----------

@dlt.create_table(
  comment="The cleaned subscribers and partitioned by sub_type",
    partition_cols=["sub_type"],
  table_properties={
    "Globalmart_deltaliv.quality": "silver",
    "pipelines.autoOptimize.managed": "true"
  }
)
@dlt.expect_all({"valid_subscriber": "sub_id IS NOT NULL "})

def subscribers_clean():
    subscribers_df = dlt.read('subscribers_raw')
    subscribers_df = subscribers_df.select([col(column).alias(column.lower()) for column in subscribers_df.columns])
    return subscribers_df

# COMMAND ----------


