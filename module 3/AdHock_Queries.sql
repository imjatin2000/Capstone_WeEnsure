-- Databricks notebook source
select * from capstone.agents_clean

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Q1: Total yearly reimbursement across all categories:

-- COMMAND ----------

SELECT
    YEAR(CAST(capstone.reimbursement_clean.reimbursement_date AS DATE)) AS year,
    ROUND(SUM(CASE WHEN capstone.claims_clean.treatment = 'Inpatient_Care' THEN capstone.reimbursement_clean.amount_approved ELSE 0 END), 2) AS inpatient_care,
    ROUND(SUM(CASE WHEN capstone.claims_clean.treatment = 'Outpatient_Care' THEN capstone.reimbursement_clean.amount_approved ELSE 0 END), 2) AS outpatient_care,
    ROUND(SUM(CASE WHEN capstone.claims_clean.treatment = 'Prescription_Drugs' THEN capstone.reimbursement_clean.amount_approved ELSE 0 END), 2) AS prescription_drugs,
    ROUND(SUM(CASE WHEN capstone.claims_clean.treatment = 'Mental_Health_Care' THEN capstone.reimbursement_clean.amount_approved ELSE 0 END), 2) AS mental_health_care,
    ROUND(SUM(CASE WHEN capstone.claims_clean.treatment = 'Dental_Care' THEN capstone.reimbursement_clean.amount_approved ELSE 0 END), 2) AS dental_care,
    ROUND(SUM(CASE WHEN capstone.claims_clean.treatment = 'Vision_Care' THEN capstone.reimbursement_clean.amount_approved ELSE 0 END), 2) AS vision_care
FROM
    capstone.claims_clean
JOIN
    capstone.reimbursement_clean ON capstone.claims_clean.claim_number = capstone.reimbursement_clean.claim_number
WHERE
    YEAR(CAST(capstone.reimbursement_clean.reimbursement_date AS DATE)) BETWEEN 2018 AND 2022
GROUP BY
    year
ORDER BY
    year;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Q2: Top 3 plans to be sold the most in each year

-- COMMAND ----------

WITH PlanSales AS (
    SELECT
        YEAR(CAST(capstone.policies_clean.policy_start_date AS DATE)) AS year,
        capstone.policies_clean.plan_id,
        COUNT(*) AS plan_sales_count
    FROM
        capstone.policies_clean
    GROUP BY
        year, capstone.policies_clean.plan_id
),
RankedPlans AS (
    SELECT
        year,
        plan_id,
        plan_sales_count,
        ROW_NUMBER() OVER (PARTITION BY year ORDER BY plan_sales_count DESC) AS rank
    FROM
        PlanSales
)
SELECT
    year,
    MAX(CASE WHEN rank = 1 THEN plan_id END) AS Rank_1,
    MAX(CASE WHEN rank = 2 THEN plan_id END) AS Rank_2,
    MAX(CASE WHEN rank = 3 THEN plan_id END) AS Rank_3
FROM
    RankedPlans
WHERE
    year BETWEEN 2018 AND 2021
GROUP BY
    year
ORDER BY
    year;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Q3: Top 10 providers to pay highest reimbursements
-- MAGIC

-- COMMAND ----------

WITH ProviderReimbursements AS (
    SELECT
        p.provider_name,
        r.amount_approved
    FROM
        capstone.reimbursement_clean r
    JOIN
        capstone.claims_clean c ON r.claim_number = c.claim_number
    JOIN
        capstone.provider_clean p ON c.provider_id = p.provider_id
)
SELECT
    provider_name,
    SUM(amount_approved) AS total_reimbursement
FROM
    ProviderReimbursements
GROUP BY
    provider_name
ORDER BY
    total_reimbursement DESC
LIMIT 10;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Q4: Agent who sold max policies

-- COMMAND ----------

WITH AgentPolicyCount AS (
    SELECT
        a.agent_id,
        a.name AS agent_name,
        COUNT(DISTINCT p.policy_number) AS policy_count
    FROM
        capstone.agents_clean a
    JOIN
        capstone.customers_clean c ON a.agent_id = c.agent_id
    JOIN
        capstone.policies_clean p ON c.customer_id = p.customer_id
    GROUP BY
        a.agent_id, a.name
)
SELECT
    agent_id,
    agent_name,
    policy_count
FROM
    AgentPolicyCount
WHERE
    policy_count = (SELECT MAX(policy_count) FROM AgentPolicyCount);


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Q5. Top 5 subscribers to buy most policies

-- COMMAND ----------

WITH SubscriberPolicyCount AS (
    SELECT
        s.sub_id,
        s.name AS subscriber_name,
        COUNT(p.policy_number) AS policy_count
    FROM
        capstone.subscribers_clean s
    JOIN
        capstone.customers_clean c ON s.sub_id = c.sub_id
    JOIN
        capstone.policies_clean p ON c.customer_id = p.customer_id
    GROUP BY
        s.sub_id, s.name
)
SELECT
    sub_id,
    subscriber_name,
    policy_count
FROM
    SubscriberPolicyCount
ORDER BY
    policy_count DESC
LIMIT 5;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Q6: Customer who made the most claims
-- MAGIC

-- COMMAND ----------

WITH CustomerClaimCount AS (
    SELECT
        c.customer_id,
        c.customer_name,
        COUNT(cl.claim_number) AS claim_count
    FROM
        capstone.customers_clean c
    RIGHT JOIN
        capstone.policies_clean p ON c.customer_id = p.customer_id
    RIGHT JOIN
        capstone.claims_clean cl ON p.policy_number = cl.policy_number
    GROUP BY
        c.customer_id, c.customer_name
)
SELECT
    customer_id,
    customer_name,
    claim_count
FROM
    CustomerClaimCount
ORDER BY
    claim_count DESC
LIMIT 1;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Q7: Customer to whom highest total amount was reimbursed
-- MAGIC

-- COMMAND ----------

WITH CustomerReimbursementTotal AS (
    SELECT
        c.customer_id,
        c.customer_name,
        SUM(r.amount_approved) AS total_reimbursement_amount
    FROM
        capstone.customers_clean c
    JOIN
        capstone.policies_clean p ON c.customer_id = p.customer_id
    JOIN
        capstone.claims_clean cl ON p.policy_number = cl.policy_number
    JOIN
        capstone.reimbursement_clean r ON cl.claim_number = r.claim_number
    GROUP BY
        c.customer_id, c.customer_name
)
SELECT
    customer_id,
    customer_name,
    total_reimbursement_amount
FROM
    CustomerReimbursementTotal
ORDER BY
    total_reimbursement_amount DESC
LIMIT 1;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Q8: Agent for whom highest number of claims were rejected (lowest claim resolution rate)

-- COMMAND ----------

WITH AgentRejectedClaims AS (
    SELECT
        a.agent_id,
        a.name,
        COUNT(*) AS rejected_claim_count
    FROM
        capstone.agents_clean a
    JOIN
        capstone.customers_clean c ON a.agent_id = c.agent_id
    JOIN
        capstone.policies_clean p ON c.customer_id = p.customer_id
    JOIN
        capstone.claims_clean cl ON p.policy_number = cl.policy_number
    JOIN
        capstone.rejected_claims_clean rc ON cl.claim_number = rc.claim_number
    GROUP BY
        a.agent_id, a.name
)
SELECT
    agent_id,
    name,
    rejected_claim_count
FROM
    AgentRejectedClaims
WHERE
    rejected_claim_count = (SELECT MAX(rejected_claim_count) FROM AgentRejectedClaims);


-- COMMAND ----------


