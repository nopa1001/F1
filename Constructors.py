# Databricks notebook source
dbutils.secrets.get("f1-scope", "f1datalkey")
f1datal_account_key = dbutils.secrets.get("f1-scope", "f1datalkey")
spark.conf.set(
    "fs.azure.account.key.f1datal.dfs.core.windows.net", f1datal_account_key)

# COMMAND ----------

display(dbutils.fs.ls('abfss://raw@f1datal.dfs.core.windows.net'))

# COMMAND ----------

constructors_schema = "constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING"

# COMMAND ----------

constructor_df = spark.read \
.schema(constructors_schema) \
.json("abfss://raw@f1datal.dfs.core.windows.net/constructors.json")


# COMMAND ----------

display(constructor_df)

# COMMAND ----------

from pyspark.sql.functions import col
constructor_dropped_df = constructor_df.drop(col('url'))

# COMMAND ----------

from pyspark.sql.functions import current_timestamp
constructor_final_df = constructor_dropped_df.withColumnRenamed("constructorId", "constructor_id") \
                                             .withColumnRenamed("constructorRef", "constructor_ref") \
                                             .withColumn("ingestion_date", current_timestamp())
display(constructor_final_df)

# COMMAND ----------

constructor_final_df.write.mode("overwrite").parquet("abfss://processed@f1datal.dfs.core.windows.net/constructors")

# COMMAND ----------

# MAGIC %md
# MAGIC
