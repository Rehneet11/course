# Databricks notebook source
# MAGIC %run "../includes/common_functions"

# COMMAND ----------

dbutils.widgets.text("p_file_date", "")
v_file_date = dbutils.widgets.get("p_file_date")
v_file_date


# COMMAND ----------


spark.sql(f"""
          CREATE TABLE IF NOT EXISTS f1_presentation.calculated_race_results(
              race_year INT,
              team_name STRING,
              driver_id INT,
              driver_name STRING,
              race_id INT,
              position INT,
              points INT,
              calculated_points INT,
              created_date TIMESTAMP,
              updated_date TIMESTAMP

          )
USING DELTA
""")

# COMMAND ----------

spark.sql("""
            CREATE OR REPLACE TEMP VIEW race_results_updates AS
            SELECT races.race_year,
                constructors.name AS team,
                drivers.driver_id,
                drivers.name AS driver_name,
                races.race_id,
                results.position,
                results.points,
                results.file_date,
                11-results.position AS calculated_points
            FROM f1_processed.results
            JOIN f1_processed.drivers
            ON (results.driver_id = drivers.driver_id)
            JOIN f1_processed.constructors
            ON (results.constructor_id = constructors.constructor_id)
            JOIN f1_processed.races
            ON (results.race_id = races.race_id)
            WHERE results.position<=10 AND results.file_date LIKE '2021-04-18'
""")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) FROM race_results_updates

# COMMAND ----------

spark.sql(f""" 
        MERGE INTO f1_presentation.calculated_race_results tgt
        USING race_results_updates src
        ON (tgt.race_id = src.race_id AND tgt.driver_id = src.driver_id)
        WHEN MATCHED THEN
        UPDATE SET tgt.position = src.position,
                    tgt.points = src.points,
                    tgt.calculated_points = src.calculated_points,
                    tgt.updated_date = current_timestamp()
        WHEN NOT MATCHED
        THEN INSERT (race_year,team_name,driver_id,driver_name,race_id,position,points,calculated_points,created_date) VALUES (src.race_year,src.team,src.driver_id,src.driver_name,src.race_id,src.position,src.points,src.calculated_points,current_timestamp) 
""") 

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) from f1_presentation.calculated_race_results
