# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

race_results_df=spark.read.parquet(f"{presentation_folder_path}/race_results")
race_results_df.display()

# COMMAND ----------

demo_df=race_results_df.filter("race_year=2020")
demo_df.display()

# COMMAND ----------

from pyspark.sql.functions import count, countDistinct, sum
demo_df.select(countDistinct('race_name')).show()

# COMMAND ----------

demo_df.filter('driver_name="Lewis Hamilton"').select(sum('points').alias('total_points'), countDistinct('race_name').alias('number_of_races')).show()

# COMMAND ----------

demo_df.groupBy('driver_name').agg(countDistinct('race_name').alias('number_of_races'), sum('points').alias('total_points')).display()

# COMMAND ----------

demo_df=race_results_df.filter("race_year in (2019,2020)")

# COMMAND ----------

demo_group_df=demo_df.groupBy('driver_name','race_year').agg(countDistinct('race_name').alias('number_of_races'), sum('points').alias('total_points'))
demo_group_df.display()

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import desc,rank
driver_rank_spec=Window.partitionBy('race_year').orderBy(desc('total_points'))
demo_group_df.withColumn('rank',rank().over(driver_rank_spec)).display()

# COMMAND ----------


