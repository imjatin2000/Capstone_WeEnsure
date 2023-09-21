# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
import dlt

# COMMAND ----------

# MAGIC %run /Users/rahul_1692255823757@npmentorskool.onmicrosoft.com/capstone/Bronze

# COMMAND ----------

@dlt.create_table(
  comment="The cleaned plans and partitioned by payment_type",
  partition_cols=["payment_type"],
  table_properties={
    "We_Ensure_deltaliv.quality": "silver",
    "pipelines.autoOptimize.managed": "true"
  }
)
@dlt.expect_all({"valid_plan_id": "plan_id IS NOT NULL "})

def plans_clean():
    plans_df =spark.read.format("delta").load("dbfs:/pipelines/850f92c8-88a8-4a57-9e5a-2080ad3f4c1e/tables/plans_raw")
    plans_df = plans_df.select([col(column).alias(column.lower()) for column in plans_df.columns])
    plans_df = plans_df.dropDuplicates(["plan_id"])
    return plans_df


# COMMAND ----------

@dlt.create_table(
  comment="The cleaned policies and partitioned by plan_id",
    partition_cols=["plan_id"],
  table_properties={
    "We_Ensure_deltaliv.quality": "silver",
    "pipelines.autoOptimize.managed": "true"
  }
)

@dlt.expect_all({"valid_policy": "policy_number IS NOT NULL "})

def policies_clean():
    policies_df = spark.read.format("delta").load("dbfs:/pipelines/850f92c8-88a8-4a57-9e5a-2080ad3f4c1e/tables/policies_raw")
    policies_df = policies_df.select([col(column).alias(column.lower()) for column in policies_df.columns])
    policies_df=policies_df.dropDuplicates(["policy_number"])
    return policies_df

# COMMAND ----------


