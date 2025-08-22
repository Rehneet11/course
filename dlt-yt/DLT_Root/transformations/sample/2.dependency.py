# import dlt
# from pyspark.sql.functions import col,lower
# #  Creating an end to end basic Pipeline
# # Staging area
# @dlt.table(
#     name="staging_table"
# )
# def staging_table():
#     df=spark.readStream.table("vsarthidlt.source.orders")
#     return df

# @dlt.view(
#     name="transform_orders"
# )
# def transform_orders():
#     df=spark.readStream.table("staging_table")
#     df=df.withColumn("order_status",lower(col("order_status")))
#     return df

# @dlt.table(
#     name="aggregated_table"
# )
# def aggregated_table():
#     df=spark.readStream.table("transform_orders")
#     df=df.groupBy("order_status").count()
#     return df