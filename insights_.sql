-- Databricks notebook source
-- MAGIC %md
-- MAGIC #Top 5 High-Value Customers
-- MAGIC

-- COMMAND ----------

SELECT c.customer_id, c.customer_name, SUM(paid_amount) AS total_payments
FROM capstone.customers_clean c
JOIN capstone.policies_clean p ON c.customer_id = p.customer_id
JOIN capstone.payments_clean py ON p.policy_number = py.policy_number
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
FROM capstone.provider_clean pr
LEFT JOIN capstone.claims_clean c ON pr.provider_id = c.provider_id
LEFT JOIN capstone.Policies_clean p ON c.policy_number = p.policy_number
GROUP BY pr.provider_name
ORDER BY policies_sold DESC
limit 5;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #Average daily premium and average insurance coverage for each plan

-- COMMAND ----------

SELECT
    pl.plan_id,
    AVG(p.daily_premium) AS avg_premium,
    round(AVG(p.insurance_coverage),2) AS avg_insurance_coverage
FROM
    capstone.policies_clean p
JOIN
    capstone.Plans_clean pl
ON
    p.plan_id = pl.plan_id
GROUP BY
    pl.plan_id
ORDER BY
    avg_premium;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #Treatments grouped by number of claims approved

-- COMMAND ----------

SELECT c.treatment, count(c.treatment) AS claims_approved
FROM capstone.Reimbursements_clean r
join capstone.claims_clean c on c.claim_number= r.claim_number
group by c.treatment

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #counting the number of subscribers associated with each plan_id - OK

-- COMMAND ----------

SELECT
    pl.plan_id,
    COUNT(DISTINCT s.sub_id) AS subscriber_count
FROM
    capstone.Plans_clean pl
JOIN
    capstone.policies_clean po ON pl.plan_id = po.plan_id
JOIN
    capstone.customers_clean cu ON po.customer_id = cu.customer_id
JOIN
    capstone.subscribers_clean s ON cu.sub_id = s.sub_id
GROUP BY
    pl.plan_id
ORDER BY
    subscriber_count DESC;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC #Total reimbursement amount for each customer
-- MAGIC

-- COMMAND ----------

SELECT cu.customer_id, cu.customer_name, round(SUM(r.amount_approved),2) AS total_reimbursement_amount
FROM capstone.customers_clean cu
JOIN capstone.Policies_clean po ON cu.customer_id = po.customer_id
JOIN capstone.claims_clean c ON po.policy_number = c.policy_number
JOIN capstone.Reimbursements_clean r ON c.claim_number = r.claim_number
GROUP BY cu.customer_id, cu.customer_name
ORDER BY total_reimbursement_amount DESC;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC #Agents who have generated the highest total premiums

-- COMMAND ----------


SELECT a.agent_id, a.name AS agent_name, SUM(p.daily_premium) AS total_premium_generated
FROM capstone.agents_clean a
LEFT JOIN capstone.customers_clean c ON a.agent_id = c.agent_id
LEFT JOIN capstone.policies_clean p ON c.customer_id = p.customer_id
GROUP BY a.agent_id, a.name
ORDER BY total_premium_generated DESC


-- COMMAND ----------

-- MAGIC %md
-- MAGIC #Number of Claims by Treatment Type
-- MAGIC

-- COMMAND ----------

SELECT C.treatment, COUNT(*) AS total_claims_new
FROM capstone.Claims_clean C
GROUP BY C.treatment;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #Average Policy Duration by Plan Type

-- COMMAND ----------

SELECT Pl.plan_id, round(AVG(DATEDIFF(P.policy_end_date, P.policy_start_date)),2) AS avg_policy_duration_new
FROM capstone.Policies_clean P
LEFT JOIN capstone.Plans_clean Pl ON P.plan_id = Pl.plan_id
GROUP BY Pl.plan_id;
