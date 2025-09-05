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
qualifying_schema=StructType(fields=[StructField("raceId", IntegerType(), True),
                                    StructField("driverId", IntegerType(), True),
                                    StructField("qualifyId", IntegerType(), True),
                                    StructField("constructorId", IntegerType(), True),
                                    StructField("number", StringType(), True),
                                    StructField("position", IntegerType(), True),
                                    StructField("q1", StringType(), True),
                                    StructField("q2", StringType(), True),
                                    StructField("q3", StringType(), True)
                                    ]
                            )

# COMMAND ----------

# MAGIC %md
# MAGIC #### Apply Schema

# COMMAND ----------

df=spark.read.schema(qualifying_schema).option("multiLine", True).json(f"{raw_folder_path}/{v_file_date}/qualifying")
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Renaming the Columns

# COMMAND ----------

from pyspark.sql.functions import current_timestamp,lit
final_df=df.withColumnRenamed("raceId","race_id") \
.withColumnRenamed("driverId","driver_id") \
.withColumnRenamed("qualifyId","qualify_id") \
.withColumnRenamed("constructorId","constructor_id")\
.withColumn("data_source",lit(v_data_source))\
.withColumn("file_date",lit(v_file_date))
final_df=add_ingestion_date(final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Write Data to Dl in parquet format

# COMMAND ----------

final_df=rearrange_partition_column(final_df,"race_id")

# COMMAND ----------

merge_condition="tgt.qualify_id=src.qualify_id"
merge_delta_data("f1_processed","qualifying",final_df,"race_id",merge_condition)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT race_id,COUNT(1)
# MAGIC FROM f1_processed.qualifying
# MAGIC GROUP BY race_id
# MAGIC ORDER BY race_id DESC
