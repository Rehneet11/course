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

from pyspark.sql.types import StructType, StructField, IntegerType,StringType, DoubleType
pit_stops_schema=StructType(fields=[StructField("raceId", IntegerType(), True),
                                    StructField("driverId", IntegerType(), True),
                                    StructField("stop", IntegerType(), True),
                                    StructField("lap", IntegerType(), True),
                                    StructField("time", StringType(), True),
                                    StructField("duration", StringType(), True),
                                    StructField("milliseconds", IntegerType(), True)
                                    ]
                            )

# COMMAND ----------

# MAGIC %md
# MAGIC #### Apply Schema

# COMMAND ----------

df=spark.read.schema(pit_stops_schema).option("multiLine", True).json(f"{raw_folder_path}/{v_file_date}/pit_stops.json")
df.display()

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

final_df=rearrange_partition_column(final_df,"race_id")

# COMMAND ----------

merge_condition="tgt.race_id=src.race_id and tgt.driver_id=src.driver_id and tgt.stop=src.stop"
merge_delta_data("f1_processed","pit_stops",final_df,"race_id",merge_condition)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT race_id, COUNT(1)
# MAGIC FROM f1_processed.pit_stops
# MAGIC GROUP BY race_id
# MAGIC ORDER BY race_id DESC
