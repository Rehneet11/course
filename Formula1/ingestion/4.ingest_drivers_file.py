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

from pyspark.sql.types import StructType,StringType,StructField,IntegerType,DateType
name_schema=StructType(fields=[StructField("forename",StringType(),True),
                               StructField("surname",StringType(),True)
                               ]
                       )
drivers_schema=StructType(fields=[StructField("code",StringType(),True),
                                  StructField("dob",DateType(),True),
                                  StructField("driverId",IntegerType(),True),
                                  StructField("name",name_schema),
                                  StructField("nationality",StringType(),True),
                                  StructField("number",IntegerType(),True),
                                  StructField("url",StringType(),True),
                                  StructField("driverRef",StringType(),True)
                                  ]
                          )

# COMMAND ----------

# MAGIC %md
# MAGIC #### Apply Schema

# COMMAND ----------

df=spark.read.schema(drivers_schema).json(f"{raw_folder_path}/{v_file_date}/drivers.json")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Rename and Add New Columns

# COMMAND ----------

from pyspark.sql.functions import col,lit,concat,current_timestamp
renamed_df=df.withColumnRenamed("driverId","driver_id")\
.withColumnRenamed("driverRef","driver_ref")\
.withColumn("name",concat(col("name.forename"),lit(" "),col('name.surname')))\
.withColumn("data_source",lit(v_data_source))\
.withColumn("file_date",lit(v_file_date))
renamed_df=add_ingestion_date(renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Drop url column

# COMMAND ----------

final_df=renamed_df.drop("url")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Write Data to Dl in parquet format

# COMMAND ----------

final_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.drivers")
