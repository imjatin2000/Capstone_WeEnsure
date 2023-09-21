# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *


# COMMAND ----------

# MAGIC %md
# MAGIC ##Data quality check

# COMMAND ----------

display(dbutils.fs.ls("/mnt/weEnsures/unzipped"))

# COMMAND ----------

# MAGIC %md
# MAGIC ##Reimbursement table

# COMMAND ----------

reimbursements_df = spark.read.option("header", "true").option("inferSchema", "true").option("multiline", "true").csv("dbfs:/mnt/weEnsures/unzipped/Reimbursement.csv")


# COMMAND ----------

display(reimbursements_df)

# COMMAND ----------

# Check for missing values
missing_values = reimbursements_df.select([col(c).alias(c+"_missing") for c in reimbursements_df.columns]).\
    selectExpr(["CASE WHEN {} IS NULL THEN 1 ELSE 0 END AS {}".format(c+"_missing", c) for c in reimbursements_df.columns])

missing_values.show()

# COMMAND ----------

missing_counts = reimbursements_df.select([col(c).alias(c+"_missing") for c in reimbursements_df.columns]).\
    selectExpr(["SUM(CASE WHEN {} IS NULL THEN 1 ELSE 0 END) AS {}".format(c+"_missing", c) for c in reimbursements_df.columns])

missing_counts.show()

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
    reimbursements_df = dlt.read("reimbursement")
    
    # Convert column names to lowercase
    reimbursements_df = reimbursements_df.select([col(column).alias(column.lower()) for column in reimbursements_df.columns])
    
    # Check for duplicacy based on claim_number and reimbursement_number
    reimbursements_df = reimbursements_df.dropDuplicates(["claim_number", "reimbursement_number"])
    
    # Cast reimbursement_date to DATE format
    reimbursements_df = reimbursements_df.withColumn("reimbursement_date", to_date(reimbursements_df["reimbursement_date"], "M/d/yyyy"))

    reimbursements_df = reimbursements_df.withColumn("amount_approved", reimbursements_df["amount_approved"].cast(DecimalType(10, 2)))
    
    return reimbursements_df


# COMMAND ----------

# MAGIC %md
# MAGIC #Provider table

# COMMAND ----------

provider_df = spark.read.option("header", "true").option("inferSchema", "true").option("multiline", "true").csv("dbfs:/mnt/weEnsures/unzipped/Provider.csv")


# COMMAND ----------

display(provider_df)

# COMMAND ----------

# Check for missing values
missing_values = provider_df.select([col(c).alias(c+"_missing") for c in provider_df.columns]).\
    selectExpr(["CASE WHEN {} IS NULL THEN 1 ELSE 0 END AS {}".format(c+"_missing", c) for c in provider_df.columns])

missing_values.show()


# COMMAND ----------

missing_counts = provider_df.select([col(c).alias(c+"_missing") for c in provider_df.columns]).\
    selectExpr(["SUM(CASE WHEN {} IS NULL THEN 1 ELSE 0 END) AS {}".format(c+"_missing", c) for c in provider_df.columns])

missing_counts.show()

# COMMAND ----------

# Filter rows with phone number length less than 10
provider_df = provider_df.filter(length(col("phone")) >= 10)


# COMMAND ----------

# No rows having phone number less than 10 digits

# COMMAND ----------

# Create a Delta Lake table for providers
@dlt.create_table(
    comment="Cleaned providers data",
    table_properties={
        "Globalmart_deltaliv.quality": "silver",
        "pipelines.autoOptimize.managed": "true"
    }
)
@dlt.expect_all({"valid_provider": "provider_id IS NOT NULL "})
def provider_clean():
    provider_df = dlt.read("provider")   
    
    # Lowercase column names
    provider_df = provider_df.select([col(column).alias(column.lower()) for column in provider_df.columns])

    # Check for duplicacy based on claim_number and reimbursement_number
    provider_df = provider_df.dropDuplicates(["provider_id"])
    
    return provider_df
