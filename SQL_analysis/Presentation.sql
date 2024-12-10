-- Databricks notebook source
USE f1_processed;


-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.secrets.get("f1-scope", "f1datalkey")
-- MAGIC f1datal_account_key = dbutils.secrets.get("f1-scope", "f1datalkey")
-- MAGIC spark.conf.set(
-- MAGIC     "fs.azure.account.key.f1datal.dfs.core.windows.net", f1datal_account_key)

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS f1_presentation;

-- COMMAND ----------

CREATE TABLE f1_presentation.calculated_race_results
USING parquet
AS
SELECT races.race_year,
       constructors.name AS team_name,
       drivers.name AS driver_name,
       results.position,
       results.points,
       11 - results.position AS calculated_points
  FROM results 
  JOIN f1_processed.drivers ON (results.driver_id = drivers.driver_id)
  JOIN f1_processed.constructors ON (results.constructor_id = constructors.constructor_id)
  JOIN f1_processed.races ON (results.race_id = races.race_id)
 WHERE results.position <= 10

-- COMMAND ----------

SELECT * FROM f1_presentation.calculated_race_results
