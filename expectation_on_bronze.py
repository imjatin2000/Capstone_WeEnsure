# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
import dlt

# COMMAND ----------

# MAGIC %run "/Users/jatin_1692255857312@npmentorskool.onmicrosoft.com/capstone/bronze"

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
    customers_df = dlt.read('customers_raw')
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
    payments_df = dlt.read('payments_raw')
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
    agents_df = dlt.read('agents_raw')
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
    claims_df = dlt.read('claims_raw')
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
    plans_df = dlt.read('plans_raw')
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
    policies_df = dlt.read('policies_raw')
    policies_df = policies_df.select([col(column).alias(column.lower()) for column in policies_df.columns])
    return policies_df

# COMMAND ----------

@dlt.create_table(
  comment="The cleaned provider ",
  table_properties={
    "Globalmart_deltaliv.quality": "silver",
    "pipelines.autoOptimize.managed": "true"
  }
)
@dlt.expect_all({"valid_provider": "provider_id IS NOT NULL "})

def provider_clean():
    provider_df = dlt.read('provider_raw')
    provider_df = provider_df.select([col(column).alias(column.lower()) for column in provider_df.columns])
    return provider_df

# COMMAND ----------

@dlt.create_table(
  comment="The cleaned reimbursement",
  table_properties={
    "Globalmart_deltaliv.quality": "silver",
    "pipelines.autoOptimize.managed": "true"
  }
)
@dlt.expect_all({"valid_claim": "claim_number IS NOT NULL "})

def reimbursement_clean():
    reimbursement_df = dlt.read('reimbursement_raw')
    reimbursement_df = reimbursement_df.select([col(column).alias(column.lower()) for column in reimbursement_df.columns])
    return reimbursement_df

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
  comment="The cleaned rejected_claims and partitioned by sub_type",
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


