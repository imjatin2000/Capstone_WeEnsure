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
# MAGIC ####Agents who have generated the highest and lowest premium amount

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT a.agent_id, a.name AS agent_name, SUM(p.daily_premium) AS total_premium_generated
# MAGIC FROM capstone.agents_clean a
# MAGIC LEFT JOIN capstone.customers_clean c ON a.agent_id = c.agent_id
# MAGIC LEFT JOIN capstone.policies_clean p ON c.customer_id = p.customer_id
# MAGIC GROUP BY a.agent_id, a.name
# MAGIC ORDER BY total_premium_generated DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ###PLANS

# COMMAND ----------

# MAGIC %md
# MAGIC ####Average daily premium and insurance coverage for each plan

# COMMAND ----------

joined_data = policies_df.join(plans_df, "plan_id", "inner")

#Grouping data and calculating the average daily premium and insurance coverage for the plans
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

#Grouping data and counting the number of subscribers in the plans
result = (
    joined_data
    .groupBy("plan_id")
    .agg(countDistinct("sub_id").alias("subscriber_count"))
    .orderBy("subscriber_count", ascending=False)
)

display(result)

# COMMAND ----------

# MAGIC %md
# MAGIC ####Policies with the highest and lowest average policy duration

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT Pl.plan_id, round(AVG(DATEDIFF(P.policy_end_date, P.policy_start_date)),2) AS avg_policy_duration_new
# MAGIC FROM capstone.Policies_clean P
# MAGIC LEFT JOIN capstone.Plans_clean Pl ON P.plan_id = Pl.plan_id
# MAGIC GROUP BY Pl.plan_id;

# COMMAND ----------

# MAGIC %md
# MAGIC ###PAYMENTS

# COMMAND ----------

# MAGIC %md
# MAGIC ####Total Payments across all Policies

# COMMAND ----------

total_payments = payments_df.select(sum("paid_amount")).collect()[0][0]
display(total_payments)

# Print the total payments
print("Total Payments Across All Policies: {:.2f}".format(total_payments))

# COMMAND ----------

# MAGIC %md
# MAGIC ###CUSTOMERS

# COMMAND ----------

# MAGIC %md
# MAGIC ####Top Paid Customers

# COMMAND ----------

joined_data = customers_df.join(policies_df, "customer_id").join(payments_df, "policy_number")

#Grouping the data and summing up the paid amount
total_payments = joined_data.groupBy("customer_id", "customer_name").agg(sum("paid_amount").alias("total_payments"))

# Order the results by total_payments in descending order and limit to the top 5
result = total_payments.orderBy(total_payments["total_payments"].desc())
display(result)

# COMMAND ----------

# MAGIC %md
# MAGIC ####Number of customers who has opted for least premium policy amount

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

#Grouping the data and counting the number of policies per provider
policies_sold = (
    joined_data
    .groupBy("provider_name")
    .agg(count("policy_number").alias("policies_sold"))
)

# Order the results by policies_sold in descending order and limit to the top 5
result = policies_sold.orderBy(policies_sold["policies_sold"].desc())
display(result)

# COMMAND ----------

# MAGIC %md
# MAGIC ###REIMBURSEMENTS

# COMMAND ----------

# MAGIC %md
# MAGIC ####Customer who got the highest reimbursement amount

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT cu.customer_id, cu.customer_name, round(SUM(r.amount_approved),2) AS total_reimbursement_amount
# MAGIC FROM capstone.customers_clean cu
# MAGIC JOIN capstone.Policies_clean po ON cu.customer_id = po.customer_id
# MAGIC JOIN capstone.claims_clean c ON po.policy_number = c.policy_number
# MAGIC JOIN capstone.Reimbursements_clean r ON c.claim_number = r.claim_number
# MAGIC GROUP BY cu.customer_id, cu.customer_name
# MAGIC ORDER BY total_reimbursement_amount DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ####Overall Average Reimbursed amount

# COMMAND ----------

average_reimbursed_amount = reimbursements_df.agg(avg("amount_approved").alias("average_reimbursed_amount")).collect()[0]["average_reimbursed_amount"]

# Show the result
print(f"Average Reimbursed Amount: Rs.{average_reimbursed_amount:.2f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ####Treatments grouped by number of claims approved

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT c.treatment, count(c.treatment) AS claims_approved
# MAGIC FROM capstone.Reimbursements_clean r
# MAGIC join capstone.claims_clean c on c.claim_number= r.claim_number
# MAGIC group by c.treatment

# COMMAND ----------

# MAGIC %md
# MAGIC ###CLAIMS

# COMMAND ----------

# MAGIC %md
# MAGIC ####Number of Claims by Treatment Type

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT C.treatment, COUNT(*) AS total_claims_new
# MAGIC FROM capstone.Claims_clean C
# MAGIC GROUP BY C.treatment;

# COMMAND ----------

# MAGIC %md
# MAGIC ####Overall Average Claim amount

# COMMAND ----------

average_claim_amount = claims_df.agg(avg("amount_claimed").alias("average_claim_amount")).collect()[0]["average_claim_amount"]

# Show the result
print(f"Average Claim Amount: Rs.{average_claim_amount:.2f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ###POLICIES

# COMMAND ----------

# MAGIC %md
# MAGIC ####Percentage of people who have paid the full amount for their policy

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
