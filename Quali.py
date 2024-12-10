# Databricks notebook source
dbutils.secrets.get("f1-scope", "f1datalkey")
f1datal_account_key = dbutils.secrets.get("f1-scope", "f1datalkey")
spark.conf.set(
    "fs.azure.account.key.f1datal.dfs.core.windows.net", f1datal_account_key)
display(dbutils.fs.ls('abfss://raw@f1datal.dfs.core.windows.net'))

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# COMMAND ----------

qualifying_schema = StructType(fields=[StructField("qualifyId", IntegerType(), False),
                                      StructField("raceId", IntegerType(), True),
                                      StructField("driverId", IntegerType(), True),
                                      StructField("constructorId", IntegerType(), True),
                                      StructField("number", IntegerType(), True),
                                      StructField("position", IntegerType(), True),
                                      StructField("q1", StringType(), True),
                                      StructField("q2", StringType(), True),
                                      StructField("q3", StringType(), True),
                                     ])

# COMMAND ----------

qualifying_df = spark.read.schema(qualifying_schema).option("multiLine", True).json("abfss://raw@f1datal.dfs.core.windows.net/qualifying/")

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

final_df = qualifying_df.withColumnRenamed("qualifyId", "qualify_id") \
.withColumnRenamed("driverId", "driver_id") \
.withColumnRenamed("raceId", "race_id") \
.withColumnRenamed("constructorId", "constructor_id") \
.withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

display(final_df)

# COMMAND ----------

final_df.write.mode("overwrite").parquet("abfss://processed@f1datal.dfs.core.windows.net/quali")
display(dbutils.fs.ls("abfss://processed@f1datal.dfs.core.windows.net/quali"))
