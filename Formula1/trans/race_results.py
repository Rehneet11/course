# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS f1_raw_2 LOCATION 'abfss://raw@udemycourse65dl.dfs.core.windows.net/'

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

dbutils.widgets.text("p_file_date","")
v_file_date=dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW v_drivers AS
# MAGIC SELECT 
# MAGIC     number AS driver_number,
# MAGIC     name AS driver_name,
# MAGIC     nationality AS driver_nationality,
# MAGIC     code,
# MAGIC     dob,
# MAGIC     driver_id,
# MAGIC     driver_ref,
# MAGIC     data_source,
# MAGIC     file_date,
# MAGIC     ingestion_date
# MAGIC FROM f1_processed.drivers;
# MAGIC SELECT * FROM v_drivers

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_processed.drivers

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW v_constructors AS
# MAGIC SELECT 
# MAGIC     name AS team,
# MAGIC     constructor_id,
# MAGIC     constructor_ref,
# MAGIC     name,
# MAGIC     nationality,
# MAGIC     data_source,
# MAGIC     file_date,
# MAGIC     ingestion_date
# MAGIC FROM f1_processed.constructors;
# MAGIC SELECT * FROM v_constructors

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW v_circuits as
# MAGIC SELECT 
# MAGIC     location as circuit_location,
# MAGIC     circuit_id,
# MAGIC     circuit_ref,
# MAGIC     name,
# MAGIC     location,
# MAGIC     country,
# MAGIC     latitude,
# MAGIC     longitude,
# MAGIC     altitude,
# MAGIC     data_source,
# MAGIC     file_date,
# MAGIC     ingestion_date
# MAGIC   FROM f1_processed.circuits;
# MAGIC
# MAGIC SELECT * FROM v_circuits
# MAGIC

# COMMAND ----------

races_df=spark.read.parquet("f1_processed.races")\
    .withColumnRenamed("name","race_name")\
    .withColumnRenamed("race_timestamp","race_date")

# COMMAND ----------

spark.sql(f"""
    CREATE OR REPLACE VIEW v_results AS
    SELECT 
        race_id AS results_race_id,
        driver_id,
        constructor_id,
        number,
        grid,
        position,
        position_text,
        position_order,
        points,
        laps,
        race_time AS race_time,
        fastest_lap,
        rank,
        fastest_lap_time,
        fastest_lap_speed,
        status_id,
        file_date
    FROM f1_processed.results
    WHERE file_date = '{v_file_date}'
""")

# COMMAND ----------

from pyspark.sql.functions import desc,current_timestamp
races_circuits_df=races_df.join(circuits_df,circuits_df.circuit_id==races_df.circuit_id,"inner")
final_df=results_df.join(races_circuits_df,races_circuits_df.race_id==results_df.results_race_id,"inner")\
.join(drivers_df,drivers_df.driver_id==results_df.driver_id,"inner")\
.join(constructors_df,constructors_df.constructor_id==results_df.constructor_id,"inner")

final_df=final_df.select(races_circuits_df.race_year.alias("race_year"),\
races_circuits_df.race_id.alias("race_id"),\
races_circuits_df.race_name.alias("race_name"),\
races_circuits_df.race_date.alias("race_date"),\
races_circuits_df.circuit_location.alias("circuit_location"),\
drivers_df.driver_name.alias("driver_name"),\
drivers_df.driver_number.alias("driver_number"),\
drivers_df.driver_nationality.alias("driver_nationality"),\
constructors_df.team.alias("team"),\
results_df.grid.alias("grid"),\
results_df.fastest_lap.alias("fastest_lap"),\
results_df.race_time.alias("race_time"),\
results_df.points.alias("points"),\
results_df.position.alias("position"))\
.withColumn("created_date",current_timestamp())
final_df.display()

# COMMAND ----------

incremental_load("f1_presentation","race_results",final_df,"race_id")

# COMMAND ----------

final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_presentation.race_results")
