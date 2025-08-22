import dlt

@dlt.view(
    name="products_staged_transformed"
)
def products_staged_tarnsformed():
    df=spark.readStream.table("products_staged");
    return df

dlt.create_streaming_table(
    name="products_transformed"
)

dlt.create_auto_cdc_flow(
    target = "products_transformed",
    source = "products_staged_transformed",
    keys = ["product_id"],
    sequence_by = "last_updated",
    ignore_null_updates = False,
    apply_as_deletes = None,
    apply_as_truncates = None,
    column_list = None,
    except_column_list = None,
    stored_as_scd_type = <type>,
    track_history_column_list = None,
    track_history_except_column_list = None,
    name = None,
    once = False
)