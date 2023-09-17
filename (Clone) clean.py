# Databricks notebook source
spark.conf.set('fs.azure.account.key.weensureadls.dfs.core.windows.net',"PEZvTff5mPF6upQkAAXFbmeSJSlSZHqzPbcgz7z5SRBzhUi1bUuvLBfsOa37M1meufUzgjZFRDoD+ASt1YNNeA==")

# COMMAND ----------

path = "abfss://batchcontainer@weensureadls.dfs.core.windows.net/unzipped/"

# COMMAND ----------

paths=(dbutils.fs.ls(path))

# COMMAND ----------

display(paths)

# COMMAND ----------

file_names = [file_info.name for file_info in paths]
print(file_names)

# COMMAND ----------


file_names = [file_info.name for file_info in paths]
dfs = {}

for file_name in file_names:
    file_path = f"{path}/{file_name}/"
    df = spark.read.format("csv").option("header", "True").option("inferSchema", "True").load(file_path)
    dfs[file_name] = df


# COMMAND ----------

dfs

# COMMAND ----------

plans_df = dfs['Plans.csv']

# COMMAND ----------

display(plans_df)

# COMMAND ----------

def distinct_count(df,column):
   x=(df.select(column).distinct().count())
   return x

distinct_count(plans_df,"plan_id")

# COMMAND ----------

def distinct_count_consistence(df,column):
   x=(df.select(column).distinct().count())
   y=(df.select(column).count())
   consistence = x == y 
   return consistence

result = distinct_count_consistence(plans_df,"plan_id")

if result:
    result = "Data is consistent."
else:
    result = "Data is not consistent."

print("Data consistency result:", result)


# COMMAND ----------

policies_df = dfs['Policies.csv']

# COMMAND ----------

display(policies_df)

# COMMAND ----------

distinct_count(policies_df,"policy_number")

# COMMAND ----------

distinct_count_consistence(policies_df,"policy_number")

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

display(policies_df.groupBy("policy_number").count().filter(col("count") > 1).select("policy_number"))

# COMMAND ----------

policies_df= policies_df.dropDuplicates()

# COMMAND ----------

distinct_count_consistence(policies_df,"policy_number")
