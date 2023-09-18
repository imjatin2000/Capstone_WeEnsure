-- Databricks notebook source
-- MAGIC %md
-- MAGIC #Customer Demographics by Plan Type

-- COMMAND ----------


SELECT P.plan_id, C.sex, COUNT(*) AS customer_count_new
FROM we_ensure_new.Policies_clean P
JOIN we_ensure_new.customers_clean C ON P.customer_id = C.customer_id
GROUP BY P.plan_id, C.sex;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC #Top 5 High-Value Customers
-- MAGIC

-- COMMAND ----------


SELECT c.customer_id, c.customer_name, SUM(paid_amount) AS total_payments
FROM we_ensure_new.customers_clean c
JOIN we_ensure_new.policies_clean p ON c.customer_id = p.customer_id
JOIN we_ensure_new.payments_clean py ON p.policy_number = py.policy_number
GROUP BY c.customer_id, c.customer_name
ORDER BY total_payments DESC
limit 5;



-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Top 5 Providers
-- MAGIC

-- COMMAND ----------


SELECT pr.provider_name,
       COUNT(DISTINCT p.policy_number) AS policies_sold
FROM we_ensure_new.provider_clean pr
LEFT JOIN we_ensure_new.claims_clean c ON pr.provider_id = c.provider_id
LEFT JOIN we_ensure_new.Policies_clean p ON c.policy_number = p.policy_number
GROUP BY pr.provider_name
ORDER BY policies_sold DESC
limit 5;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC #Claim Rejection reason Analysis

-- COMMAND ----------


SELECT rc.reason, COUNT(*) AS rejection_count
FROM we_ensure_new.Rejected_claims_clean rc
GROUP BY rc.reason
ORDER BY rejection_count DESC
LIMIT 5;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC #calculating the average daily premium and the average insurance coverage for each plan

-- COMMAND ----------


SELECT
    pl.plan_id,
    AVG(p.daily_premium) AS avg_premium,
    round(AVG(p.insurance_coverage),2) AS avg_insurance_coverage
FROM
    we_ensure_new.policies_clean p
JOIN
    we_ensure_new.Plans_clean pl
ON
    p.plan_id = pl.plan_id
GROUP BY
    pl.plan_id
ORDER BY
    avg_premium;



-- COMMAND ----------

-- MAGIC %md
-- MAGIC #calculating the total payments made for insurance claims on an annual basis

-- COMMAND ----------


SELECT YEAR(c.claim_date) AS claim_year, round(SUM(paid_amount),2) AS total_payments
FROM we_ensure_new.claims_clean c
JOIN we_ensure_new.payments_clean py ON c.policy_number = py.policy_number
GROUP BY claim_year
ORDER BY claim_year;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC #Claims with the Highest  Approved

-- COMMAND ----------


SELECT claim_number, amount_approved AS highest_approved_amount
FROM we_ensure_new.Reimbursements_clean
WHERE amount_approved = (
    SELECT MAX(amount_approved)
    FROM we_ensure_new.Reimbursements_clean
)



-- COMMAND ----------

-- MAGIC %md
-- MAGIC #Claims with the Lowest Amounts Approved

-- COMMAND ----------


SELECT claim_number, amount_approved AS lowest_approved_amount
FROM we_ensure_new.Reimbursements_clean
WHERE amount_approved = (
    SELECT MIN(amount_approved)
    FROM we_ensure_new.Reimbursements_clean
);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #most common subscription types

-- COMMAND ----------


SELECT sub_type, COUNT(*) AS customer_count
FROM we_ensure_new.subscribers_clean
GROUP BY sub_type
ORDER BY customer_count DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #counting the number of subscribers associated with each plan_id

-- COMMAND ----------

SELECT
    pl.plan_id,
    COUNT(DISTINCT s.sub_id) AS subscriber_count
FROM
    we_ensure_new.Plans_clean pl
JOIN
    we_ensure_new.policies_clean po ON pl.plan_id = po.plan_id
JOIN
    we_ensure_new.customers_clean cu ON po.customer_id = cu.customer_id
JOIN
    we_ensure_new.subscribers_clean s ON cu.sub_id = s.sub_id
GROUP BY
    pl.plan_id
ORDER BY
    subscriber_count DESC;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC #The total reimbursement amount for each customer
-- MAGIC

-- COMMAND ----------

SELECT cu.customer_id, cu.customer_name, round(SUM(r.amount_approved),2) AS total_reimbursement_amount
FROM we_ensure_new.customers_clean cu
JOIN we_ensure_new.Policies_clean po ON cu.customer_id = po.customer_id
JOIN we_ensure_new.claims_clean c ON po.policy_number = c.policy_number
JOIN we_ensure_new.Reimbursements_clean r ON c.claim_number = r.claim_number
GROUP BY cu.customer_id, cu.customer_name
ORDER BY total_reimbursement_amount DESC;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC #The total number of customers assigned to each agent

-- COMMAND ----------


SELECT a.agent_id, a.name AS agent_name, COUNT(c.customer_id) AS total_customers
FROM we_ensure_new.Agents_clean a
LEFT JOIN we_ensure_new.customers_clean c ON a.agent_id = c.agent_id
GROUP BY a.agent_id, a.name
ORDER BY total_customers DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #The percentage of claimed amounts that have been successfully paid

-- COMMAND ----------


SELECT CONCAT(ROUND(SUM(pa.paid_amount) / SUM(c.amount_claimed) * 100, 2), '%') AS payment_recovery_rate
FROM we_ensure_new.payments_clean pa
JOIN we_ensure_new.claims_clean c ON pa.policy_number = c.policy_number;



-- COMMAND ----------

-- MAGIC %md
-- MAGIC #The top agents who have generated the highest total premiums
-- MAGIC

-- COMMAND ----------


SELECT a.agent_id, a.name AS agent_name, SUM(p.daily_premium) AS total_premium_generated
FROM we_ensure_new.agents_clean a
LEFT JOIN we_ensure_new.customers_clean c ON a.agent_id = c.agent_id
LEFT JOIN we_ensure_new.policies_clean p ON c.customer_id = p.customer_id
GROUP BY a.agent_id, a.name
ORDER BY total_premium_generated DESC
LIMIT 10;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC #The frequency of claims by policy type  to identify which plans have the highest and lowest claim rates

-- COMMAND ----------


SELECT p.plan_id, COUNT(DISTINCT c.claim_number) AS claim_count
FROM we_ensure_new.policies_clean p
LEFT JOIN we_ensure_new.claims_clean c ON p.policy_number = c.policy_number
GROUP BY p.plan_id
ORDER BY claim_count DESC;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC #Number of Claims by Treatment Type
-- MAGIC

-- COMMAND ----------


SELECT C.treatment, COUNT(*) AS total_claims_new
FROM we_ensure_new.Claims_clean C
GROUP BY C.treatment;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC #Average Policy Duration by Plan Type
-- MAGIC

-- COMMAND ----------

SELECT Pl.plan_id, round(AVG(DATEDIFF(P.policy_end_date, P.policy_start_date)),2) AS avg_policy_duration_new
FROM we_ensure_new.Policies_clean P
LEFT JOIN we_ensure_new.Plans_clean Pl ON P.plan_id = Pl.plan_id
GROUP BY Pl.plan_id;


-- COMMAND ----------


