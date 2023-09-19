# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
import dlt

# COMMAND ----------

agents_df = spark.read.format("delta").table("capstone.agents_clean")
claims_df = spark.read.format("delta").table("capstone.claims_clean")
customers_df = spark.read.format("delta").table("capstone.customers_clean")
payments_df = spark.read.format("delta").table("capstone.payments_clean")
plans_df = spark.read.format("delta").table("capstone.plans_clean")
policies_df = spark.read.format("delta").table("capstone.policies_clean")
provider_df = spark.read.format("delta").table("capstone.provider_clean")
reimbursements_df = spark.read.format("delta").table("capstone.reimbursements_clean")
rejected_claims_df = spark.read.format("delta").table("capstone.rejected_claims_clean")
subscribers_df = spark.read.format("delta").table("capstone.subscribers_clean")

# COMMAND ----------

agents_df.createOrReplaceTempView("agents_clean")
claims_df.createOrReplaceTempView("claims_clean")
customers_df.createOrReplaceTempView("customers_clean")
payments_df.createOrReplaceTempView("payments_clean")
plans_df.createOrReplaceTempView("plans_clean")
policies_df.createOrReplaceTempView("policies_clean")
provider_df.createOrReplaceTempView("provider_clean")
reimbursements_df.createOrReplaceTempView("reimbursements_clean")
rejected_claims_df.createOrReplaceTempView("rejected_claims_clean")
subscribers_df.createOrReplaceTempView("subscribers_clean")

# COMMAND ----------

# MAGIC %md
# MAGIC ###AGENTS

# COMMAND ----------

# MAGIC %md
# MAGIC ####Agents working with Max customers

# COMMAND ----------

joined_data = agents_df.join(customers_df,'agent_id')

#Grouping data and counting the number of customer the agents have worked with
customers_agents = joined_data.groupBy(["agent_id",'name']).count().orderBy(desc("count"))
display(customers_agents)

#Finding the name of the agents with max and min number of customers
best_agent = customers_agents.select("name").orderBy(col("count").desc()).limit(1)
worst_agent = customers_agents.select("name").orderBy(col("count"))

best_agent_value = best_agent.first()["name"]
worst_agent_value = worst_agent.first()['name']

#showing the result
print(f"The agent who worked with most customers: {best_agent_value}")
print(f"The agent who worked with least customers: {worst_agent_value}")

# COMMAND ----------

# MAGIC %md
# MAGIC ###PLANS

# COMMAND ----------

# MAGIC %md
# MAGIC ####Average daily premium and insurance coverage for each plan

# COMMAND ----------

joined_data = policies_df.join(plans_df, "plan_id", "inner")

result = (
    joined_data
    .groupBy("plan_id")
    .agg(
        round(avg("plans_clean.daily_premium"), 2).alias("avg_premium"),
        round(avg("insurance_coverage"), 2).alias("avg_insurance_coverage")
    )
    .orderBy("avg_premium")
)

display(result)

# COMMAND ----------

# MAGIC %md
# MAGIC ####Number of subscribers for each Plan

# COMMAND ----------

joined_data = (
    plans_df
    .join(policies_df, "plan_id", "inner")
    .join(customers_df, "customer_id", "inner")
    .join(subscribers_df, "sub_id", "inner")
)

result = (
    joined_data
    .groupBy("plan_id")
    .agg(countDistinct("sub_id").alias("subscriber_count"))
    .orderBy("subscriber_count", ascending=False)
)

display(result)

# COMMAND ----------

# MAGIC %md
# MAGIC ###PAYMENTS

# COMMAND ----------

# MAGIC %md
# MAGIC ####Total Payments across all Policies

# COMMAND ----------

total_payments = payments_df.select(sum("paid_amount"))
display(total_payments)

# Print the total payments
#print("Total Payments Across All Policies: {:.2f}".format(total_payments))

# COMMAND ----------

# MAGIC %md
# MAGIC ###CUSTOMERS

# COMMAND ----------

# MAGIC %md
# MAGIC ####Top Paid Customers

# COMMAND ----------

joined_data = customers_df.join(policies_df, "customer_id").join(payments_df, "policy_number")
total_payments = joined_data.groupBy("customer_id", "customer_name").agg(sum("paid_amount").alias("total_payments"))

# Order the results by total_payments in descending order and limit to the top 5
result = total_payments.orderBy(total_payments["total_payments"].desc())
display(result)

# COMMAND ----------

# MAGIC %md
# MAGIC ###PROVIDERS

# COMMAND ----------

# MAGIC %md
# MAGIC ####Top 5 Providers

# COMMAND ----------

joined_data = (
    provider_df
    .join(claims_df, "provider_id", "left")
    .join(policies_df, "policy_number", "left")
)

policies_sold = (
    joined_data
    .groupBy("provider_name")
    .agg(count("policy_number").alias("policies_sold"))
)

# Order the results by policies_sold in descending order and limit to the top 5
result = policies_sold.orderBy(policies_sold["policies_sold"].desc())
display(result)

# COMMAND ----------

joined_data = (
    policies_df
    .join(plans_df, "plan_id", "inner")
    .join(customers_df, "customer_id", "inner")
)

# Find the minimum premium amount
min_premium = joined_data.select(min("plans_clean.daily_premium")).first()[0]

# Filter customers with the least premium policy
customers_with_least_premium = (
    joined_data
    .filter(joined_data["plans_clean.daily_premium"] == min_premium)
    .select("customer_id")
    .distinct()
)

# Count the number of customers with the least premium policy
num_customers_with_least_premium = customers_with_least_premium.count()

# Show the result
print(f"Number of customers with the least premium policy: {num_customers_with_least_premium}")


# COMMAND ----------

joined_df = payments_df.join(policies_df, "policy_number", "inner")

# Calculate the total amount paid for each policy
total_paid_amount = joined_df.groupBy("policy_number").agg(sum("paid_amount").alias("total_paid_amount"))

# Calculate the total amount due for each policy
total_due_amount = joined_df.groupBy("policy_number").agg(sum("daily_premium").alias("total_due_amount"))

# Calculate whether each policy is fully paid or not
fully_paid_df = total_paid_amount.join(total_due_amount, "policy_number", "inner") \
    .withColumn("fully_paid", when(col("total_paid_amount") >= col("total_due_amount"), 1).otherwise(0))

# Calculate the percentage of people who have paid the full amount
fully_paid_percentage = (fully_paid_df.filter(col("fully_paid") == 1).count() / fully_paid_df.count()) * 100

# Show the result
print(f"Percentage of people who have paid the full amount for their policy: {fully_paid_percentage:.2f}%")

# COMMAND ----------

average_claim_amount = claims_df.agg(avg("amount_claimed").alias("average_claim_amount")).collect()[0]["average_claim_amount"]

# Show the result
print(f"Average Claim Amount: Rs.{average_claim_amount:.2f}")

# COMMAND ----------

average_reimbursed_amount = reimbursements_df.agg(avg("amount_approved").alias("average_reimbursed_amount")).collect()[0]["average_reimbursed_amount"]

# Show the result
print(f"Average Claim Amount: Rs.{average_reimbursed_amount:.2f}")
