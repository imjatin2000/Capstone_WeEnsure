# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
import dlt

# COMMAND ----------

# @dlt.create_table(
#   comment="The cleaned customers, ingested from delta and partitioned by city",
#   partition_cols=["city"],
#   table_properties={
#     "Globalmart_deltaliv.quality": "silver",
#     "pipelines.autoOptimize.managed": "true"
#   }
# )
# def customers_clean():
#     customers_df = spark.read.format("delta").load("dbfs:/pipelines/cb7e2f6c-7567-4318-9171-0b8c8e5772db/tables/customers_raw")
#     customers_df = customers_df.select([col(column).alias(column.lower()) for column in customers_df.columns])
#     return customers_df

# COMMAND ----------

@dlt.create_table(
  comment="Cleaned reimbursement data partitioned by reimbursement_date",
  partition_cols=["reimbursement_date"],
  table_properties={
    "Globalmart_deltaliv.quality": "silver",
    "pipelines.autoOptimize.managed": "true"
  }
)
def reimbursements_clean():
    reimbursements_df = spark.read.format("delta").load("dbfs:/pipelines/ac104ec9-13a4-4654-b446-fec15f8fadf4/tables/reimbursement")
    
    # Convert column names to lowercase
    reimbursements_df = reimbursements_df.select([col(column).alias(column.lower()) for column in reimbursements_df.columns])
    
    # Check for duplicacy based on claim_number and reimbursement_number
    reimbursements_df = reimbursements_df.dropDuplicates(["claim_number", "reimbursement_number"])
    
    # Cast reimbursement_date to DATE format
    reimbursements_df = reimbursements_df.withColumn("reimbursement_date", to_date(reimbursements_df["reimbursement_date"], "M/d/yyyy"))
    
    return reimbursements_df


# COMMAND ----------

# Create a Delta Lake table for providers
@dlt.create_table(
    comment="Cleaned providers data",
    table_properties={
        "Globalmart_deltaliv.quality": "silver",
        "pipelines.autoOptimize.managed": "true"
    }
)
def provider_clean():
    provider_df = spark.read.format("delta").load("dbfs:/pipelines/ac104ec9-13a4-4654-b446-fec15f8fadf4/tables/provider")    
    
    # Lowercase column names
    provider_df = provider_df.select([col(column).alias(column.lower()) for column in provider_df.columns])

    # Check for duplicacy based on claim_number and reimbursement_number
    provider_df = provider_df.dropDuplicates(["provider_id"])
    
    return provider_df
