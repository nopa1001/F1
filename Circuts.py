# Databricks notebook source
dbutils.secrets.get("f1-scope", "f1datalkey")
f1datal_account_key = dbutils.secrets.get("f1-scope", "f1datalkey")
spark.conf.set(
    "fs.azure.account.key.f1datal.dfs.core.windows.net", f1datal_account_key)

# COMMAND ----------

display(dbutils.fs.ls('abfss://raw@f1datal.dfs.core.windows.net'))

# COMMAND ----------

from pyspark.sql.types import StructType , StructField, IntegerType, StringType, DoubleType

circutis_schema = StructType([StructField("circuitId", IntegerType(), True),
                                      StructField("name", StringType(), True),
                                      StructField("location", StringType(), True),
                                      StructField("country", StringType(), True),
                                      StructField("lat", DoubleType(), True),
                                      StructField("lng", DoubleType(), True),
                                      StructField("alt", DoubleType(), True),
                                      StructField("url", StringType(), True)
                                      ])

# COMMAND ----------


circuits_df = spark.read.option("header", True).schema(circuts_schema).csv('abfss://raw@f1datal.dfs.core.windows.net/circuits.csv')
display(circuits_df)

# COMMAND ----------

from pyspark.sql.functions import col
circuits_df_selected = circuits_df.select(col('circuitId'), col('name'), col('location'), col('country'), col('lat'), col('lng'), col('alt'))
display(circuts_df_selected)

# COMMAND ----------

circuits_renamed_df = circuts_df_selected.withColumnRenamed("circuitId", "circuit_id") \
.withColumnRenamed("circuitRef", "circuit_ref") \
.withColumnRenamed("lat", "latitude") \
.withColumnRenamed("lng", "longitude") \
.withColumnRenamed("alt", "altitude")
display(circuits_renamed_df)


# COMMAND ----------

from pyspark.sql.functions import current_timestamp
circuits_final_df = circuits_renamed_df.withColumn("ingestion_date", current_timestamp())
display(circuits_final_df)

# COMMAND ----------

circuits_final_df.write.mode("overwrite").parquet("abfss://processed@f1datal.dfs.core.windows.net/circuits")

# COMMAND ----------

df = spark.read.parquet("abfss://processed@f1datal.dfs.core.windows.net/circuits")
display(df)
