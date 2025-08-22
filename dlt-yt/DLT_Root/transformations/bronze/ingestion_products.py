import dlt

rules = {
    "rule1": "product_id IS NOT NULL",
    "rule2": "product_name IS NOT NULL",
    "rule3": "price >= 0"
}

@dlt.table(
    name="products_staged"
)

@dlt.expect_all_or_drop(rules)

def products_staged():
    df = spark.readStream.table("vsarthidlt.dwh.products")
    return df