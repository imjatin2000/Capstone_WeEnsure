# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
import dlt

# COMMAND ----------

# MAGIC %run "/Users/jatin_1692255857312@npmentorskool.onmicrosoft.com/capstone/expectation_on_bronze_and _Cleaning"

# COMMAND ----------

# MAGIC %md
# MAGIC ## PROVIDER FACTS

# COMMAND ----------

def calculate_total_profit(provider_df, claims_df, reimbursement_df, rejected_claims_df):
    # Filter out claims that are not in rejected claims
    valid_claims_df = claims_df.join(rejected_claims_df, "claim_number", "left_anti")
    
    # Calculate profit
    profit_df = valid_claims_df.join(reimbursement_df, "claim_number", "left") \
        .groupBy("provider_id") \
        .agg(sum(when(reimbursement_df.amount_approved.isNull(), 0)
                 .otherwise(valid_claims_df.amount_claimed - reimbursement_df.amount_approved)).alias("total_profit"))
    
    return profit_df

# COMMAND ----------

def calculate_average_settlement_time(claims_df, reimbursement_df, rejected_claims_df):
    # Join claims with reimbursement and rejected claims
    joined_df = claims_df.join(reimbursement_df, "claim_number", "left") \
                        .join(rejected_claims_df, "claim_number", "left")

    # Calculate the difference between claim_date and reimbursement_date
    time_difference_df = joined_df.withColumn("settlement_time",
                                              when(col("reason").isNull(),
                                                   when(reimbursement_df.amount_approved.isNull(), 0)
                                                   .otherwise(datediff(reimbursement_df.reimbursement_date, claims_df.claim_date))
                                                  )
                                              )
    
    # Group by provider_id and calculate the average settlement time
    avg_settlement_time_df = time_difference_df.groupBy("provider_id") \
                                              .agg(avg("settlement_time").alias("average_settlement_time"))

    return avg_settlement_time_df

# COMMAND ----------

def calculate_total_reimburse_claims(claims_df, reimbursement_df, rejected_claims_df):
    # Join claims with reimbursement and rejected claims
    joined_df = claims_df.join(reimbursement_df, "claim_number", "left") \
                        .join(rejected_claims_df, "claim_number", "left")

    # Calculate the number of reimbursed claims, excluding rejected claims
    total_reimburse_claims_df = joined_df \
        .groupBy("provider_id") \
        .agg(count(when(col("reason").isNull() & col("amount_approved").isNotNull(), 1)).alias("total_reimburse_claims"))

    return total_reimburse_claims_df

# COMMAND ----------

def calculate_total_rejected_claims(claims_df, rejected_claims_df):
    return claims_df.join(rejected_claims_df, "claim_number", "inner") \
                    .groupBy("provider_id") \
                    .agg(sum(when(rejected_claims_df.reason.isNotNull(), 1).otherwise(0)).alias("total_rejected_claims"))

# COMMAND ----------

@dlt.create_table(
  comment="The provider aggregated facts",
  table_properties={
    "WeEnusre.quality": "gold",
    "pipelines.autoOptimize.managed": "true"
  }
)

def providers_agg_facts():
    """
    Calculate aggregated facts about providers based on various data sources.

    This function performs several data operations on provider-related data, including calculating
    total profit, average settlement time, total reimburse claims, and total rejected claims. It
    then aggregates these facts into a single DataFrame.

    Returns:
        DataFrame: A DataFrame containing aggregated facts about providers.

    Raises:
        None

    Dependencies:
        - dlt: Reading provider_clean data
        - calculate_total_profit: A function to calculate total profit for providers.
        - calculate_average_settlement_time: A function to calculate the average settlement time for claims.
        - calculate_total_reimburse_claims: A function to calculate the total reimburse claims.
        - calculate_total_rejected_claims: A function to calculate the total rejected claims.
    """
    provider_df = dlt.read("provider_clean")
    claims_df = dlt.read("claims_clean")
    reimbursement_df = dlt.read("reimbursement_clean")
    rejected_claims_df = dlt.read("rejected_claims_clean")
    
    total_profit_df = calculate_total_profit(provider_df, claims_df, reimbursement_df,rejected_claims_df)
    average_settlement_time_df = calculate_average_settlement_time(claims_df, reimbursement_df,rejected_claims_df)
    total_reimburse_claims_df = calculate_total_reimburse_claims(claims_df, reimbursement_df,rejected_claims_df)
    total_rejected_claims_df=calculate_total_rejected_claims(claims_df,rejected_claims_df)

    
    provider_facts = total_profit_df \
    .join(average_settlement_time_df, "provider_id", "inner") \
    .join(total_reimburse_claims_df, "provider_id", "inner") \
    .join(total_rejected_claims_df, "provider_id", "inner")

    return provider_facts

# COMMAND ----------

# MAGIC %md
# MAGIC # AGENTS FACTS

# COMMAND ----------

def calculate_policies_sold_per_month(customers_df, policies_df):
    # Join the 'customers' and 'policies' DataFrames on 'customer_id'
    joined_df = customers_df.join(policies_df, 'customer_id', 'inner')

    # Calculate the month from 'policy_start_date'
    joined_df = joined_df.withColumn("start_month", month("policy_start_date"))

    # Calculate the number of policies sold per month by agent
    policies_sold_per_month_df = joined_df.groupBy("agent_id", "start_month").agg(count("policy_number").alias("policies_sold"))

    return policies_sold_per_month_df

def calculate_total_premium_collected(customers_df, policies_df):
    # Join the 'customers' and 'policies' DataFrames on 'customer_id'
    joined_df = customers_df.join(policies_df, 'customer_id', 'inner')

    # Calculate the total premium collected by summing the 'daily_premium' column
    total_premium_collected_df = joined_df.groupBy("agent_id").agg(sum("daily_premium").alias("total_premium_collected"))

    return total_premium_collected_df


def calculate_renewing_customers(customers_df, policies_df):
    # Join the 'customers' and 'policies' DataFrames on 'customer_id'
    joined_df = customers_df.join(policies_df, 'customer_id', 'inner')

    # Filter for customers with a valid 'policy_start_date'
    filtered_df = joined_df.filter(policies_df.policy_start_date.isNotNull())

    # Calculate the total number of renewing customers per agent
    renewing_customers_df = filtered_df.groupBy("agent_id").agg(count("customer_id").alias("number_of_renewing_customers"))

    return renewing_customers_df


# COMMAND ----------

@dlt.create_table(
  comment="The agents aggregated facts",
  table_properties={
    "WeEnusre.quality": "gold",
    "pipelines.autoOptimize.managed": "true"
  }
)

def agents_agg_facts():
    """
    Calculate aggregated facts about agents based on various data sources.

    This function performs several data operations on agent-related data, including calculating
    the total number of policies sold per month, renewing customers, and total premium collected.
    It then aggregates these facts into a single DataFrame.

    Returns:
        DataFrame: A DataFrame containing aggregated facts about agents.

    Raises:
        None

    Dependencies:
        - dlt: A library/module for reading data.
        - calculate_policies_sold_per_month: A function to calculate the total policies sold per month.
        - calculate_renewing_customers: A function to calculate renewing customers.
        - calculate_total_premium_collected: A function to calculate the total premium collected.
    """
    agents_df = dlt.read("agents_clean")
    policies_df=dlt.read("policies_clean")
    customers_df=dlt.read("customers_clean")
    
    total_polices_per_month_df=calculate_policies_sold_per_month(customers_df,policies_df)
    renewing_customers_df = calculate_renewing_customers(customers_df, policies_df)
    total_premium_collected_df = calculate_total_premium_collected(customers_df, policies_df)

    # Join the three DataFrames
    agents_facts = total_premium_collected_df.join(renewing_customers_df, "agent_id", "left") \
        .join(total_polices_per_month_df, "agent_id", "left")

    return agents_facts

# COMMAND ----------

# MAGIC %md
# MAGIC # CUSTOMERS FACT 

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
  comment="The aggregate customers facts",
  table_properties={
    "WeEnsure.quality": "gold",
    "pipelines.autoOptimize.managed": "true"
  }
)

def customers_agg_facts():
    """
    Compute aggregated facts about customers based on various data sources.

    This function performs several data operations on customer-related data, including policy renewals,
    claim frequencies, premium-to-claim ratios, settlement days, reimbursement details, rejected claims,
    and invalid claims. It then aggregates these facts into a single DataFrame.

    Returns:
        DataFrame: A DataFrame containing aggregated facts about customers.

    Raises:
        None

    Dependencies:
        - dlt: Reading customer_clean data
        - policy_renewals: A function to calculate policy renewals.
        - claim_freq: A function to calculate claim frequencies.
        - prem_to_claim_ratio: A function to calculate premium-to-claim ratios.
        - avg_settlement_days: A function to calculate average settlement days.
        - reimb_to_claims: A function to calculate reimbursement-to-claims amounts.
        - claims_rejected: A function to calculate total claims rejected.
        - invalid_claims: A function to identify invalid claims.
    """

    customers_df = dlt.read("customers_clean")

    policyRenewals_df = policy_renewals(customers_df)
    
    policy_df = dlt.read("policies_clean")
    claims_df = dlt.read("claims_clean")
    policyXclaim = claims_df.join(policy_df,claims_df["policy_number"]==policy_df["policy_number"],"inner")
    claimFrequency_df = claim_freq(policyXclaim)

    premiumToClaimRatio_df = prem_to_claim_ratio(policyXclaim)

    reimbursment_df =dlt.read("reimbursement_clean")
    reimbursementXpolicyclaim = policyXclaim.join(reimbursment_df,"claim_number","inner")
    reim_df =reimbursementXpolicyclaim.select("customer_id","claim_date","reimbursement_date")
    AvgSettlementDays_df = avg_settlement_days(reim_df)

    InvalidClaims_df = invalid_claims(policyXclaim)

    reim2_df = reimbursementXpolicyclaim.select("customer_id","amount_approved", "amount_claimed")
    reimbusementToClaimsAmount_df = reimb_to_claims(reim2_df)

    rejected_claims_df = dlt.read("rejected_claims_clean")
    rejXpolicyXclaim = policyXclaim.join(rejected_claims_df,"claim_number","inner")
    TotalClaimsRejected_df = claims_rejected(rejXpolicyXclaim)

    customers_agg_df = policyRenewals_df.join(claimFrequency_df,"customer_id","left").join(premiumToClaimRatio_df,"customer_id","left").join(AvgSettlementDays_df,"customer_id","left").join(reimbusementToClaimsAmount_df,"customer_id","left").join(TotalClaimsRejected_df,"customer_id","left").join(InvalidClaims_df,"customer_id","left")
    return customers_agg_df


# COMMAND ----------

# MAGIC %md
# MAGIC # STREAMING FACTS

# COMMAND ----------

# Function to calculate daily metrics
def calculate_daily_metrics(df):

    """
    Calculate daily metrics for a streaming DataFrame.

    This function takes a streaming DataFrame 'df' containing health-related data
    and calculates daily metrics for each customer. It extracts the date from the
    'activity_timestamp' column and calculates average heart rate, total steps,
    and total calories burned for each customer on a daily basis.

    Parameters:
    - df (DataFrame): The input streaming DataFrame containing health-related data.

    Returns:
    - DataFrame: A DataFrame with daily metrics for each customer, including columns:
      - customer_id: The customer's unique identifier.
      - avg_heart_rate: The average heart rate for the day.
      - total_steps: The total number of steps taken during the day.
      - total_calories_burned: The total calories burned during the day.

    Notes:
    - This function uses the 'activity_timestamp' column to extract the date and
      group the data by customer and date.
    - It calculates daily metrics with a watermark of 10 minutes for handling late
      arriving data.
    """
    # Extract date from timestamp
    df = df.withColumn("date", date_format(col("activity_timestamp"), "yyyy-MM-dd"))
    
    # Calculate daily metrics
    daily_metrics = df.withWatermark("EventProcessedUtcTime", "10 minutes").groupBy("customer_id", "date").agg(
        avg(col("heart_rate")).alias("avg_heart_rate"),
        sum(col("total_steps")).alias("total_steps"),
        sum(col("calories")).alias("total_calories_burned")
    )\
    .drop("date")
    
    return daily_metrics

# COMMAND ----------

@dlt.create_table(
comment="The aggragate customers fact tables of the activity of customer like steps,heart rate",
table_properties={
    "WeEnsure.quality": "gold",
    "pipelines.autoOptimize.managed": "true"
}
)

def customer_stream_daily_agg_facts():
    customer_stream = dlt.read_stream('customer_stream_clean')
    daily_metrics=calculate_daily_metrics(customer_stream)
   
    # merged_results.write.format('delta').mode("overwrite").save("/mnt/path")

    return daily_metrics

# COMMAND ----------

def calculate_weekly_metrics(df):
    """
    Calculate weekly metrics for a streaming DataFrame.

    This function takes a streaming DataFrame 'df' containing health-related data
    and calculates weekly metrics for each customer. It extracts the date from the
    'activity_timestamp' column and calculates average heart rate, total steps,
    and total calories burned for each customer on a weekly basis.

    Parameters:
    - df (DataFrame): The input streaming DataFrame containing health-related data.

    Returns:
    - DataFrame: A DataFrame with weekly metrics for each customer, including columns:
      - customer_id: The customer's unique identifier.
      - week_start_date: The start date of the week.
      - avg_heart_rate: The average heart rate for the week.
      - total_steps: The total number of steps taken during the week.
      - total_calories_burned: The total calories burned during the week.

    Notes:
    - This function uses the 'activity_timestamp' column to extract the date and
      group the data by customer and the start date of each week.
    - It calculates weekly metrics with a watermark of 10 minutes for handling late
      arriving data.
    """
    # Extract the start date of the week from the timestamp
    df = df.withColumn("week_start_date", date_format(date_trunc("week", col("activity_timestamp")), "yyyy-MM-dd"))
    
    # Calculate weekly metrics
    weekly_metrics = df.withWatermark("EventProcessedUtcTime", "10 minutes").groupBy("customer_id", "week_start_date").agg(
        avg(col("heart_rate")).alias("avg_heart_rate"),
        sum(col("total_steps")).alias("total_steps"),
        sum(col("calories")).alias("total_calories_burned")
    )\
    .drop("week_start_date")
    
    return weekly_metrics


# COMMAND ----------

@dlt.create_table(
comment="The aggragate customers fact tables of the activity of customer like steps,heart rate",
table_properties={
    "WeEnsure.quality": "gold",
    "pipelines.autoOptimize.managed": "true"
}
)

def customer_stream_weekly_agg_facts():
    customer_stream = dlt.read_stream('customer_stream_clean')
    weekly_metrics=calculate_weekly_metrics(customer_stream)
   
    # merged_results.write.format('delta').mode("overwrite").save("/mnt/path")

    return weekly_metrics
