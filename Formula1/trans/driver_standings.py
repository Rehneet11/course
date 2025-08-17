# Databricks notebook source
# MAGIC %run "../includes/common_functions"

# COMMAND ----------

dbutils.widgets.text("p_file_date","")
v_file_date=dbutils.widgets.get("p_file_date")

# COMMAND ----------

race_results_df=spark.table(f"f1_presentation.race_results")
race_results_df.display()


# COMMAND ----------

race_year_list=df_col_to_list(race_results_df,'race_year')
race_year_list

# COMMAND ----------

race_results_df=spark.table(f"f1_presentation.race_results").filter(col("race_year"). isin (race_year_list))
race_results_df.display()

# COMMAND ----------

from pyspark.sql.functions import sum, count, when, col

driver_standings_df = race_results_df \
.groupBy("race_year","driver_name","driver_nationality")\
.agg(
    sum("points").alias("total_points"),
    count(when(col("position") == 1, True)).alias("wins")
)
driver_standings_df.display()

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import desc,rank
driver_standings_spec=Window.partitionBy("race_year").orderBy(desc("total_points"),desc("wins"))
final_df=driver_standings_df.withColumn("rank",rank().over(driver_standings_spec))

# COMMAND ----------

merge_condition="tgt.driver_name=src.driver_name and tgt.race_year=src.race_year"
merge_delta_data("f1_presentation","driver_standings",final_df,"race_year",merge_condition)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_presentation.driver_standings WHERE race_year = 2021
