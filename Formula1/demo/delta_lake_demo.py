# Databricks notebook source
# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS f1_demo
# MAGIC MANAGED LOCATION "abfss://temp@dbvsarthi.dfs.core.windows.net"

# COMMAND ----------

results_df=spark.read\
    .option('inferSchema','true')\
    .option('header','true')\
    .json(f"{raw_folder_path}/2021-03-28/results.json")

# COMMAND ----------

results_df.write.format("delta").mode("overwrite").saveAsTable("f1_demo.results_managed")

# COMMAND ----------

results_df.write.format("delta").mode("overwrite").save("abfss://temp@dbvsarthi.dfs.core.windows.net/results_external")

# COMMAND ----------

df=spark.read.format("delta").load("abfss://temp@dbvsarthi.dfs.core.windows.net/results_external")
df.display()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.results_managed

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS f1_demo.results_external
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://temp@dbvsarthi.dfs.core.windows.net/results_external'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.results_external

# COMMAND ----------

df=spark.read.format("delta").load("abfss://temp@dbvsarthi.dfs.core.windows.net/results_external")
df.display()

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE f1_demo.results_managed
# MAGIC SET points = 11 - position
# MAGIC WHERE position <=10;
# MAGIC     
# MAGIC SELECT * FROM f1_demo.results_managed

# COMMAND ----------

from delta.tables import DeltaTable
deltaTable = DeltaTable.forPath(spark, "abfss://temp@dbvsarthi.dfs.core.windows.net/results_external")
deltaTable.update('position <= 10', {'position': '21-position'})

# COMMAND ----------

from pyspark.sql.functions import col
drivers_day1=spark.read\
    .option('inferSchema','true')\
    .option('header','true')\
    .json(f"{raw_folder_path}/2021-03-28/drivers.json")\
    .filter("driverId<=10")\
    .select("driverId","dob", col("name.forename").alias("forename"), col("name.surname").alias("surname"))

drivers_day1.display()

# COMMAND ----------

from pyspark.sql.functions import upper
driver_day_2=spark.read\
    .option('inferSchema','true')\
    .option('header','true')\
    .json(f"{raw_folder_path}/2021-03-28/drivers.json")\
    .filter("driverId BETWEEN 6 AND 15")\
    .select("driverId","dob",upper("name.forename").alias("forename"),upper("name.surname").alias("surname"))
driver_day_2.display()

# COMMAND ----------

driver_day3=spark.read\
    .option('inferSchema','true')\
    .option('header','true')\
    .json(f"{raw_folder_path}/2021-03-28/drivers.json")\
    .filter("driverId BETWEEN 1 AND 5 OR driverId BETWEEN 16 AND 20")\
    .select("driverId","dob",upper("name.forename").alias("forename"),upper("name.surname").alias("surname"))
driver_day3.display()

# COMMAND ----------

drivers_day1.createOrReplaceTempView("drivers_day1")
driver_day_2.createOrReplaceTempView("driver_day_2")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS f1_demo.drivers_merge
# MAGIC (
# MAGIC   driverId INT,
# MAGIC   dob DATE, 
# MAGIC   forename STRING, 
# MAGIC   surname STRING, 
# MAGIC   createdDate DATE,
# MAGIC   updatedDate DATE
# MAGIC )
# MAGIC USING DELTA

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLE EXTENDED IN f1_demo LIKE 'drivers_merge'

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO f1_demo.drivers_merge tgt
# MAGIC USING drivers_day1 upd
# MAGIC ON tgt.driverId = upd.driverId
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET tgt.dob = upd.dob,
# MAGIC              tgt.forename = upd.forename,
# MAGIC              tgt.surname = upd.surname,
# MAGIC              tgt.updatedDate = current_timestamp()
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT (driverId, dob, forename,surname,createdDate)
# MAGIC   VALUES (driverId, dob, forename, surname, current_timestamp())
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.drivers_merge
# MAGIC

# COMMAND ----------

# MAGIC %sql 
# MAGIC MERGE INTO f1_demo.drivers_merge tgt
# MAGIC USING driver_day_2 upd
# MAGIC ON tgt.driverId = upd.driverId
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET tgt.dob = upd.dob,
# MAGIC              tgt.forename = upd.forename,
# MAGIC              tgt.surname = upd.surname,
# MAGIC              tgt.updatedDate = current_timestamp()
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT (driverId, dob, forename,surname,createdDate)
# MAGIC   VALUES (driverId, dob, forename, surname, current_timestamp())

# COMMAND ----------

from pyspark.sql.functions import current_timestamp
from delta.tables import DeltaTable
deltaTable=DeltaTable.forName(spark,"f1_demo.drivers_merge")
deltaTable.alias("tgt").merge(
    driver_day3.alias("upd"),
    "tgt.driverId = upd.driverId")\
    .whenMatchedUpdate(set = {
        "dob": "upd.dob", 
        "forename": "upd.forename", 
        "surname": "upd.surname", 
        "updatedDate": current_timestamp()})\
    .whenNotMatchedInsert(values = {
        "driverId": "upd.driverId", 
        "dob": "upd.dob", 
        "forename": "upd.forename", 
        "surname": "upd.surname",
        "createdDate":current_timestamp()}
    )\
.execute()

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY f1_demo.drivers_merge
