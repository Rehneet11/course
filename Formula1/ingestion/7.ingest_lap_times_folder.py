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

from pyspark.sql.types import StructType, StructField, IntegerType,StringType
lap_times_schema=StructType(fields=[StructField("raceId", IntegerType(), True),
                                    StructField("driverId", IntegerType(), True),
                                    StructField("position", IntegerType(), True),
                                    StructField("lap", IntegerType(), True),
                                    StructField("time", StringType(), True),
                                    StructField("milliseconds", IntegerType(), True)
                                    ]
                            )

# COMMAND ----------

# MAGIC %md
# MAGIC #### Apply Schema

# COMMAND ----------

df=spark.read.schema(lap_times_schema).csv(f"{raw_folder_path}/{v_file_date}/lap_times")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Renaming the Columns

# COMMAND ----------

from pyspark.sql.functions import current_timestamp,lit
final_df=df.withColumnRenamed("raceId","race_id") \
.withColumnRenamed("driverId","driver_id")\
.withColumn("data_source",lit(v_data_source))\
.withColumn("file_date",lit(v_file_date))
final_df=add_ingestion_date(final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Write Data to Dl in parquet format

# COMMAND ----------

final_df=rearrange_partition_column(final_df,'race_id')

# COMMAND ----------

# Deduplicate the source DataFrame based on the merge keys
deduped_df = final_df.dropDuplicates([
    "driver_id",
    "race_id",
    "lap"
])

merge_condition = (
    "tgt.driver_id=src.driver_id AND "
    "tgt.race_id=src.race_id AND "
    "tgt.lap=src.lap"
)
merge_delta_data(
    "f1_processed",
    "lap_times",
    deduped_df,
    "race_id",
    merge_condition
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT race_id,count(1)
# MAGIC from f1_processed.lap_times
# MAGIC group by race_id
# MAGIC order by race_id DESC
