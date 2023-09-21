# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
import dlt

# COMMAND ----------

def policy_renewals(df):
    df = spark.table("capstone.policies_clean")
    policy_renewals = df.groupBy("customer_id").agg(count("policy_number").alias("Total_Policy_Renewals"))
    return policy_renewals

# COMMAND ----------

def claim_freq(df):
    policy_df = spark.table("capstone.policies_clean")
    claims_df = spark.table("capstone.claims_clean")
    df = claims_df.join(policy_df,claims_df["policy_number"]==policy_df["policy_number"],"inner")
    df = df.withColumn("quarter", quarter(col("claim_date")))
    df = df.withColumn("year", year(col("claim_date")))

    # Group by customer_id, quarter, and year and count the number of claims
    claims_count = df.groupBy("customer_id", "quarter").agg(count("*").alias("Number_of_Claims"))
    claims_avg = claims_count.groupBy("customer_id").agg(avg("Number_of_Claims").alias("AverageClaimFrequency"))
    return claims_avg

# COMMAND ----------

def prem_to_claim_ratio(df):
    df = df.withColumn("quarter", quarter(col("claim_date")))
    
    grouped_df = df.groupBy("customer_id", "quarter").agg(
        sum("daily_premium").alias("total_premium"),
        count("claim_number").alias("num_of_claims")
    )

    # Calculate the ratio of total_premium to num_of_claims
    grouped_df = grouped_df.withColumn("premium_to_claims_ratio", col("total_premium") / col("num_of_claims"))

    # Calculate the quarterly average of the ratios
    quarterly_avg_ratio = grouped_df.groupBy("customer_id").agg(avg("premium_to_claims_ratio").alias("quarterly_avg_ratio"))

    # Show the result DataFrame
    return quarterly_avg_ratio

# COMMAND ----------

def avg_settlement_days(df):
    df = df.withColumn("days_to_settle_claim", datediff(col("reimbursement_date"), col("claim_date")))

    # Group by customer_id and calculate the average days taken to settle claims
    average_settlement_days_df = df.groupBy("customer_id").agg(avg("days_to_settle_claim").alias("average_settlement_days"))

    # Show the result DataFrame
    return average_settlement_days_df

# COMMAND ----------

def invalid_claims(df):
    df = df.withColumn("isInvalid", when(col("amount_claimed")>col("insurance_coverage"),1).otherwise(0))
    invalid_df = df.groupBy("customer_id").agg(sum("isInvalid").alias("invalidClaims"))
    return invalid_df

# COMMAND ----------

def reimb_to_claims(df):
    x = df.groupBy("customer_id").agg(sum("amount_approved").alias("total_reimbursmant_amount"))
    y = df.groupBy("customer_id").agg(sum("amount_claimed").alias("total_amount_claimed"))
    df = x.join(y,"customer_id","inner")
    df = df.withColumn("reimbursmant_to_claim_amount_ratio",col("total_reimbursmant_amount")/col("total_amount_claimed")).select("customer_id","reimbursmant_to_claim_amount_ratio")
    return df

# COMMAND ----------

def claims_rejected(df):
    df = df.groupBy("customer_id").agg(count("customer_id").alias("total_claims_rejected"))
    return df

# COMMAND ----------

@dlt.create_table(
  comment="The cleaned customers, ingested from delta and partitioned by agent_id",
  table_properties={
    "Globalmart_deltaliv.quality": "gold",
    "pipelines.autoOptimize.managed": "true"
  }
)

def CustomerFacts():
    customers_df = spark.table("capstone.customers_clean")
    policyRenewals_df = policy_renewals(customers_df)

    policy_df = spark.table("capstone.policies_clean")
    claims_df = spark.table("capstone.claims_clean")
    policyXclaim = claims_df.join(policy_df,claims_df["policy_number"]==policy_df["policy_number"],"inner")
    claimFrequency_df = claim_freq(policyXclaim)

    premiumToClaimRatio_df = prem_to_claim_ratio(policyXclaim)

    reimbursment_df = spark.table("capstone.reimbursement_clean")
    reimbursementXpolicyclaim = policyXclaim.join(reimbursment_df,"claim_number","inner")
    reim_df =reimbursementXpolicyclaim.select("customer_id","claim_date","reimbursement_date")
    AvgSettlementDays_df = avg_settlement_days(reim_df)

    InvalidClaims_df = invalid_claims(policyXclaim)

    reim2_df = reimbursementXpolicyclaim.select("customer_id","amount_approved", "amount_claimed")
    reimbusementToClaimsAmount_df = reimb_to_claims(reim2_df)

    rejected_claims_df = spark.table("capstone.rejected_claims_clean")
    rejXpolicyXclaim = policyXclaim.join(rejected_claims_df,"claim_number","inner")
    TotalClaimsRejected_df = claims_rejected(rejXpolicyXclaim)

    customers_agg_df = policyRenewals_df.join(claimFrequency_df,"customer_id","left").join(premiumToClaimRatio_df,"customer_id","left").join(AvgSettlementDays_df,"customer_id","left").join(reimbusementToClaimsAmount_df,"customer_id","left").join(TotalClaimsRejected_df,"customer_id","left").join(InvalidClaims_df,"customer_id","left")
    return customers_agg_df

# COMMAND ----------


