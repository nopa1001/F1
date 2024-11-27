# Databricks notebook source
dbutils.secrets.get("f1-scope", "f1datalkey")
f1datal_account_key = dbutils.secrets.get("f1-scope", "f1datalkey")
spark.conf.set(
    "fs.azure.account.key.f1datal.dfs.core.windows.net", f1datal_account_key)
display(dbutils.fs.ls('abfss://raw@f1datal.dfs.core.windows.net'))

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# COMMAND ----------

pit_stops_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                                      StructField("driverId", IntegerType(), True),
                                      StructField("stop", StringType(), True),
                                      StructField("lap", IntegerType(), True),
                                      StructField("time", StringType(), True),
                                      StructField("duration", StringType(), True),
                                      StructField("milliseconds", IntegerType(), True)
                                     ])

# COMMAND ----------

pit_stops_df = spark.read \
.schema(pit_stops_schema) \
.option("multiLine", True) \
.json("abfss://raw@f1datal.dfs.core.windows.net/pit_stops.json")

# COMMAND ----------

display(pit_stops_df)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

final_df = pit_stops_df.withColumnRenamed("driverId", "driver_id") \
.withColumnRenamed("raceId", "race_id") \
.withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

final_df.write.mode("overwrite").parquet("abfss://processed@f1datal.dfs.core.windows.net/pt_stops")
display(dbutils.fs.ls("abfss://processed@f1datal.dfs.core.windows.net/pt_stops"))

# COMMAND ----------

races_df = spark.read.parquet("abfss://processed@f1datal.dfs.core.windows.net/pt_stops")

display(races_df)
