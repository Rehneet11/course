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
# MAGIC #### Applying Custom Schema

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,IntegerType,StringType,DateType
race_schema=StructType(fields=[StructField("raceId",IntegerType(),False),
                               StructField("year",IntegerType(),True),
                               StructField("round",IntegerType(),True),
                               StructField("circuitId",IntegerType(),True),
                               StructField("name",StringType(),True),
                               StructField("date",DateType(),True),
                               StructField("time",StringType(),True),
                               StructField("url",StringType(),True)
                               ]
                       )

# COMMAND ----------

df=spark.read.options(header=True).schema(race_schema).csv(f"{raw_folder_path}/{v_file_date}/races.csv")
df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Select Columns

# COMMAND ----------

from pyspark.sql.functions import col,lit
selected_df=df.select(col("raceId"),col("year"),col("round"),col("circuitId"),col("name"),col("date"),col("time"))
selected_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Renaming the columns

# COMMAND ----------

renamed_df=selected_df.withColumnRenamed("raceId","race_id")\
.withColumnRenamed("year","race_year")\
.withColumnRenamed("circuitId","circuit_id")\
.withColumn("data_source",lit(v_data_source))\
.withColumn("file_date",lit(v_file_date))
renamed_df.display()

# COMMAND ----------

from pyspark.sql.functions import col,concat,lit,to_timestamp
combined_column_df=renamed_df.withColumn("race_timestamp",to_timestamp(concat(col("date"),lit(" "),col("time")), "yyyy-MM-dd HH:mm:ss"))
combined_column_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Add ingestion date

# COMMAND ----------

final_df=add_ingestion_date(combined_column_df)
final_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Writing Data in DL in Parquet Format

# COMMAND ----------

final_df.write.mode("overwrite").partitionBy("race_year").format("delta").saveAsTable("f1_processed.races")
