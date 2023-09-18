# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
import dlt

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
@dlt.expect_all({"valid_provider": "provider_id IS NOT NULL "})
def provider_clean():
    provider_df = dlt.read("provider")   
    
    # Lowercase column names
    provider_df = provider_df.select([col(column).alias(column.lower()) for column in provider_df.columns])

    # Check for duplicacy based on claim_number and reimbursement_number
    provider_df = provider_df.dropDuplicates(["provider_id"])
    
    return provider_df

# COMMAND ----------

# Create a Delta Lake table for subscribers
@dlt.create_table(
    comment="Cleaned subscribers data",
    partition_cols=[],
    table_properties={
        "Globalmart_deltaliv.quality": "silver",
        "pipelines.autoOptimize.managed": "true"
    }
)
def subscribers_clean():
    subscribers_df = dlt.read("subscribers") 

    # Data cleaning and transformations

    # STEP 1: Replace "/" with ","
    subscribers_df = subscribers_df.withColumn("phone", regexp_replace(col("phone"), "/", ","))

    # STEP 2: Remove whitespaces
    subscribers_df = subscribers_df.withColumn("phone", regexp_replace(col("phone"), "\\s+", ""))

    # STEP 3: Remove characters from the phone column except numeric, ",", and "+"
    subscribers_df = subscribers_df.withColumn("phone", regexp_replace(col("phone"), "[^0-9,\\+]", ""))

    #STEP 4: Lowercase column names
    subscribers_df = subscribers_df.select([col(column).alias(column.lower()) for column in subscribers_df.columns])

    return subscribers_df


# COMMAND ----------

@dlt.create_table(
  comment="The cleaned customers, ingested from delta and partitioned by agent_id",
  partition_cols=["agent_id"],
  table_properties={
    "Globalmart_deltaliv.quality": "silver",
    "pipelines.autoOptimize.managed": "true"
  }
)

@dlt.expect_all_or_drop({"valid_customer": "customer_id IS NOT NULL "})
def customers_clean():
    customers_df = dlt.read('customers')
    customers_df = customers_df.select([col(column).alias(column.lower()) for column in customers_df.columns])
    return customers_df

# COMMAND ----------

@dlt.create_table(
  comment="The cleaned payments, ingested from delta and partitioned by paid_amount",
  partition_cols=["paid_amount"],
  table_properties={
    "Globalmart_deltaliv.quality": "silver",
    "pipelines.autoOptimize.managed": "true"
  }
)

@dlt.expect_all({"valid_paid_amount": "paid_amount > 0"})
def payments_clean():
    payments_df = dlt.read('payment')
    payments_df = payments_df.select([col(column).alias(column.lower()) for column in payments_df.columns])
    return payments_df

# COMMAND ----------

@dlt.create_table(
  comment="The cleaned agents",
  table_properties={
    "Globalmart_deltaliv.quality": "silver",
    "pipelines.autoOptimize.managed": "true"
  }
)

@dlt.expect_all({"valid_agent": "agent_id IS NOT NULL "})
def agents_clean():
    agents_df = dlt.read('agents')
    agents_df = agents_df.select([col(column).alias(column.lower()) for column in agents_df.columns])
    return agents_df

# COMMAND ----------

@dlt.create_table(
  comment="The cleaned claims and partitioned by provider_id",
    partition_cols=["provider_id"],
  table_properties={
    "Globalmart_deltaliv.quality": "silver",
    "pipelines.autoOptimize.managed": "true"
  }
)


@dlt.expect_all({"valid_provider": "provider_id IS NOT NULL ","valid_policy": "policy_number IS NOT NULL ","valid_claim": "claim_number IS NOT NULL "})

def claims_clean():
    claims_df = dlt.read('claims')
    claims_df = claims_df.select([col(column).alias(column.lower()) for column in claims_df.columns])
    return claims_df

# COMMAND ----------

@dlt.create_table(
  comment="The cleaned plans and partitioned by payment_type",
    partition_cols=["payment_type"],
  table_properties={
    "Globalmart_deltaliv.quality": "silver",
    "pipelines.autoOptimize.managed": "true"
  }
)


@dlt.expect_all({"valid_plan_id": "plan_id IS NOT NULL "})

def plans_clean():
    plans_df = dlt.read('plans')
    plans_df = plans_df.select([col(column).alias(column.lower()) for column in plans_df.columns])
    return plans_df

# COMMAND ----------

@dlt.create_table(
  comment="The cleaned policies and partitioned by plan_id",
    partition_cols=["plan_id"],
  table_properties={
    "Globalmart_deltaliv.quality": "silver",
    "pipelines.autoOptimize.managed": "true"
  }
)

@dlt.expect_all({"valid_policy": "policy_number IS NOT NULL "})

def policies_clean():
    policies_df = dlt.read('policies')
    policies_df = policies_df.select([col(column).alias(column.lower()) for column in policies_df.columns])
    return policies_df

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
    rejected_claims_df = dlt.read('rejected_claims')
    rejected_claims_df = rejected_claims_df.select([col(column).alias(column.lower()) for column in rejected_claims_df.columns])
    return rejected_claims_df
