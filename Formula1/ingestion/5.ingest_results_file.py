# Databricks notebook source
# MAGIC %run "../includes/common_functions"

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source=dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "")
v_file_date=dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Define Schema

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,IntegerType,StringType,DoubleType
results_schema=StructType(fields=[StructField("resultId",IntegerType(),False),
                                  StructField("raceId",IntegerType(),True),
                                  StructField("driverId",IntegerType(),True),
                                  StructField("constructorId",IntegerType(),True),
                                  StructField("number",IntegerType(),True),
                                  StructField("grid",IntegerType(),True),
                                  StructField("position",IntegerType(),True),
                                  StructField("positionText",StringType(),True),
                                  StructField("positionOrder",IntegerType(),True),
                                  StructField("points",DoubleType(),True),
                                  StructField("laps",IntegerType(),True),
                                  StructField("time",StringType(),True),
                                  StructField("milliseconds",IntegerType(),True),
                                  StructField("fastestLap",IntegerType(),True),
                                  StructField("rank",IntegerType(),True),
                                  StructField("fastestLapTime",StringType(),True),
                                  StructField("fastestLapSpeed",StringType(),True),
                                  StructField("statusId",IntegerType(),True)
                                  ]
                          )

# COMMAND ----------

# MAGIC %md
# MAGIC #### Apply Schema

# COMMAND ----------

df=spark.read.schema(results_schema).json(f"{raw_folder_path}/{v_file_date}/results.json")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Rename Columns

# COMMAND ----------

from pyspark.sql.functions import current_timestamp,lit
renamed_df=df.withColumnRenamed("resultId","result_id")\
.withColumnRenamed("raceId","race_id")\
.withColumnRenamed("driverId","driver_id")\
.withColumnRenamed("constructorId","constructor_id")\
.withColumnRenamed("positionText","position_text")\
.withColumnRenamed("positionOrder","position_order")\
.withColumnRenamed("fastestLap","fastest_lap")\
.withColumnRenamed("fastestLapTime","fastest_lap_time")\
.withColumnRenamed("fastestLapSpeed","fastest_lap_speed")\
.withColumn("data_source",lit(v_data_source))\
.withColumn("file_date",lit(v_file_date))
renamed_df=add_ingestion_date(renamed_df)
renamed_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Drop Columns

# COMMAND ----------

final_df=renamed_df.drop("statusId")
final_df=final_df.dropDuplicates(["race_id","driver_id"])

# COMMAND ----------

# MAGIC %md
# MAGIC ### Method 1

# COMMAND ----------

# for race_id_list in final_df.select("race_id").distinct().collect():
#     if(spark.catalog.tableExists("f1_processed.results")):
#         spark.sql(f"ALTER TABLE f1_processed.results DROP IF EXISTS PARTITION (race_id = {race_id_list.race_id})")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Partition Data by race_id and write it to Dl in parquet format

# COMMAND ----------

# final_df.write.partitionBy("race_id").mode("append").format("parquet").saveAsTable("f1_processed.results")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Method 2

# COMMAND ----------

final_df=rearrange_partition_column(final_df,"race_id")


# COMMAND ----------

merge_condition="tgt.result_id = src.result_id AND tgt.race_id = src.race_id"
merge_delta_data("f1_processed","results",final_df,"race_id",merge_condition)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT race_id, COUNT(1)
# MAGIC FROM f1_processed.results
# MAGIC GROUP BY race_id
# MAGIC ORDER BY race_id DESC
