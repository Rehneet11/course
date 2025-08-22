# import dlt
# # Creating Streaming Table
# @dlt.table(
#     name="first_streaming_table"
# )

# def first_streaming_table():
#     df=spark.readStream.table("vsarthidlt.source.orders")
#     return df

# # Materialized Views
# @dlt.table(
#     name="first_materialized_view"
# )
# def first_materialized_view():
#     df=spark.read.table("vsarthidlt.source.orders")
#     return df

# #Create Batch View
# @dlt.view(
#     name="first_batch_view"
# )
# def first_batch_view():
#     df=spark.read.table("vsarthidlt.source.orders")
#     return df
    
# #Create Streaming View
# @dlt.view(
#     name="first_streaming_view"
# )
# def first_streaming_view():
#     df=spark.readStream.table("vsarthidlt.source.orders")
#     return df
