-- Databricks notebook source
DROP DATABASE IF EXISTS f1_processed CASCADE;

-- COMMAND ----------

DROP DATABASE IF EXISTS f1_presentation CASCADE;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.ls("abfss://raw@dbvsarthi.dfs.core.windows.net")

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS f1_processed
MANAGED LOCATION "abfss://processed@dbvsarthi.dfs.core.windows.net/"

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS f1_presentation
MANAGED LOCATION "abfss://presentation@dbvsarthi.dfs.core.windows.net/"


-- COMMAND ----------


