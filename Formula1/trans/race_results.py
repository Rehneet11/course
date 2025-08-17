# Databricks notebook source
# MAGIC %run "../includes/common_functions"

# COMMAND ----------

dbutils.widgets.text("p_file_date","")
v_file_date=dbutils.widgets.get("p_file_date")

# COMMAND ----------

driver_df=spark.table('f1_processed.drivers')\
    .withColumnRenamed("number","driver_number")\
    .withColumnRenamed("name","driver_name")\
    .withColumnRenamed("nationality","driver_nationality")
driver_df.display()

# COMMAND ----------

constructor_df=spark.table('f1_processed.constructors')\
    .withColumnRenamed("name","team")
constructor_df.display()

# COMMAND ----------

circuits_df=spark.table("f1_processed.circuits")\
    .withColumnRenamed("location","circuit_location")
circuits_df.display()

# COMMAND ----------

races_df=spark.table("f1_processed.races")\
    .withColumnRenamed("name","race_name")\
    .withColumnRenamed("race_timestamp","race_date")
races_df.display()

# COMMAND ----------


results_df=spark.table("f1_processed.results")\
    .filter(f"file_date='{v_file_date}'")\
    .withColumnRenamed("time","race_time")\
    .withColumnRenamed("race_id","results_race_id")\
    .withColumnRenamed("file_date","results_file_date")


# COMMAND ----------

from pyspark.sql.functions import desc,current_timestamp
races_circuits_df=races_df.join(circuits_df,circuits_df.circuit_id==races_df.circuit_id,"inner")\
    .select(races_df.race_id,races_df.race_year,races_df.race_name,races_df.race_date,circuits_df.circuit_location)


final_df=results_df.join(races_circuits_df,races_circuits_df.race_id==results_df.results_race_id,"inner")\
.join(driver_df,driver_df.driver_id==results_df.driver_id,"inner")\
.join(constructor_df,constructor_df.constructor_id==results_df.constructor_id,"inner")

final_df=final_df.select("race_id","race_year","race_name","race_date","circuit_location","driver_name","driver_number","driver_nationality","team","grid","fastest_lap","race_time","points","position","results_file_date")\
    .withColumn("created_date",current_timestamp())\
    .withColumnRenamed("results_file_date","file_date")

# COMMAND ----------

merge_condition="tgt.driver_name=src.driver_name and tgt.race_id=src.race_id"
merge_delta_data("f1_presentation","race_results",final_df,"race_id",merge_condition)

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT (file_date)
# MAGIC FROM f1_presentation.race_results
