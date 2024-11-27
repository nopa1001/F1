# Databricks notebook source
dbutils.secrets.get("f1-scope", "f1datalkey")
f1datal_account_key = dbutils.secrets.get("f1-scope", "f1datalkey")
spark.conf.set(
    "fs.azure.account.key.f1datal.dfs.core.windows.net", f1datal_account_key)
display(dbutils.fs.ls('abfss://raw@f1datal.dfs.core.windows.net'))

# COMMAND ----------

from pyspark.sql.types import StructType , StructField, IntegerType, StringType, TimestampType, DateType
races_schema = StructType([StructField("raceId", IntegerType(), True),
                          StructField("year", IntegerType(), True),
                          StructField("round", IntegerType(), True),
                          StructField("circuitId", IntegerType(), True),
                          StructField("name", StringType(), True),
                          StructField("date", DateType(), True),
                          StructField("time", StringType(), True),
                          StructField("url", StringType(), True)])

# COMMAND ----------

races_df = spark.read.option("header", True).schema(races_schema).csv('abfss://raw@f1datal.dfs.core.windows.net/races.csv')
display(races_df)

# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp, lit, concat, to_timestamp

races_with_timestamp = races_df.withColumn("infestion_date", current_timestamp()) \
  .withColumn("race_timestamp", to_timestamp(concat(col("date"), lit(" "), col("time")), 'yyyy-MM-dd HH:mm:ss'))
display(races_with_timestamp)

# COMMAND ----------

races_selected = races_with_timestamp.select(col('raceId').alias('race_id'), col('year').alias('race_year'), col('round'), col('circuitId').alias('circuit_id'),col('name'), col('infestion_date'), col('race_timestamp'))
display(races_selected)

# COMMAND ----------

races_selected.write.mode("overwrite").parquet("abfss://processed@f1datal.dfs.core.windows.net/races")
display(dbutils.fs.ls("abfss://processed@f1datal.dfs.core.windows.net/races"))

# COMMAND ----------

races_df = spark.read.parquet("abfss://processed@f1datal.dfs.core.windows.net/races")

display(races_df)
