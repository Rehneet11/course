# Databricks notebook source
# MAGIC %run "../includes/common_functions"

# COMMAND ----------

race_results_df=spark.table("f1_presentation.race_results")

# COMMAND ----------

from pyspark.sql.functions import sum, count, when, col
constructor_standings_df=race_results_df.groupBy("race_year","team")\
.agg(sum("points").alias("total_points"),count(when(col("position") == 1, 1)).alias("wins"))

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import desc,rank
constructor_standings_spec=Window.partitionBy("race_year").orderBy(desc("total_points"),desc("wins"))
final_df=constructor_standings_df.withColumn("rank",rank().over(constructor_standings_spec))


# COMMAND ----------

merge_condition="tgt.race_year = src.race_year AND tgt.team = src.team"
merge_delta_data("f1_presentation","constructor_standings",final_df,"race_year",merge_condition)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_presentation.constructor_standings WHERE race_year = 2021
