# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
import dlt

# COMMAND ----------

display(dbutils.fs.ls("dbfs:/pipelines/85aa7f4b-4766-42e1-bea6-94c85f5d5391/tables"))

# COMMAND ----------

@dlt.create_table(
  comment="The deep cleaned rejected_claims and partitioned by year, month and reason",
    partition_cols=["rejection_year","rejection_month","reason"],
  table_properties={
    "Globalmart_deltaliv.quality": "silver",
    "pipelines.autoOptimize.managed": "true"
  }
)

def rejected_claims_deepclean():
    rejected_claims_df =  spark.read.format("delta").load("dbfs:/pipelines/85aa7f4b-4766-42e1-bea6-94c85f5d5391/tables/rejected_claims_clean")
    
    #Convert column names to lowercase 
    rejected_claims_df = rejected_claims_df.select([col(column).alias(column.lower()) for column in rejected_claims_df.columns])

    #Check for duplicacy based on claim_number and rejection_id
    rejected_claims_df = rejected_claims_df.dropDuplicates(["claim_number", "rejection_id"])

    #Creating year and month columns to get partitioned
    rejected_claims_df = rejected_claims_df.withColumn("rejection_year", year("rejection_date")) \
                                 .withColumn("rejection_month", month("rejection_date"))

    #Check if reasons are not null values or having improper values
    reason_count = rejected_claims_df.groupBy(col("reason")).count()
    display(reason_count)

    return rejected_claims_df

# COMMAND ----------

@dlt.create_table(
  comment="The deep cleaned subscribers and partitioned by sub_type",
    partition_cols=["sub_type"],
  table_properties={
    "Globalmart_deltaliv.quality": "silver",
    "pipelines.autoOptimize.managed": "true"
  }
)

def subscribers_deepclean():
    subscribers_df =  spark.read.format("delta").load("dbfs:/pipelines/85aa7f4b-4766-42e1-bea6-94c85f5d5391/tables/subscribers_clean")

    # Convert column names to lowercase
    subscribers_df = subscribers_df.select([col(column).alias(column.lower()) for column in subscribers_df.columns])

    def standardize_phone_numbers(column):
        # Remove commas, spaces, and dashes
        cleaned_phone = regexp_replace(column,  "[(\\s\\-\\)]", "")
        cleaned_phone = regexp_replace(cleaned_phone, "\\+91\\s*", "")
        cleaned_phone = regexp_replace(cleaned_phone,  ",", "/")
        standardized_phone = regexp_replace(cleaned_phone, "(\\d{11})([^/])", "$1/$2")
        return standardized_phone

    # Apply the UDF to the phone number column
    subscribers_df = subscribers_df.withColumn("phone", standardize_phone_numbers(col("phone")))
    subscribers_df = subscribers_df.withColumn("phone", regexp_replace(col("phone"), r"/(\d{8})/", r"/022$1/"))

    #Final Deepcleaned Data
    display(subscribers_df)

    #Check if subtype are not null values or having improper values
    subtype_count = subscribers_df.groupBy(col("sub_type")).count()
    display(subtype_count)

    #Checking for values which is null but not recognized in mail column
    filtered_df = subscribers_df.filter(subscribers_df["mail"]=='-')
    display(filtered_df)

    #Changing the improper value to null value for calculation purposes
    subscribers_df = subscribers_df.withColumn("mail",
                            when(col("mail").contains("-"), None)
                            .otherwise(col("mail")))
    
    #Extracting Domain from email and checking for improper values
    domain_df = subscribers_df.withColumn("domain", regexp_extract(col("mail"), r"@([\w\.]+)", 1))
    domain_count = domain_df.groupBy(col("domain")).count()
    display(domain_count)

    return subscribers_df
