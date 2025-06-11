from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit

def get_products_and_categories(products_df, categories_df, product_category_df):
    product_with_categories = products_df.alias("p") \
        .join(product_category_df.alias("pc"), col("p.id") == col("pc.product_id"), how="left") \
        .join(categories_df.alias("c"), col("pc.category_id") == col("c.id"), how="left") \
        .select(
            col("p.id").alias("product_id"),
            col("p.name").alias("product_name"),
            col("c.name").alias("category_name")
        )

    products_with_categories_only = product_with_categories.filter(col("category_name").isNotNull())

    products_without_categories = product_with_categories.filter(col("category_name").isNull()) \
        .select("product_name").distinct()

    return products_with_categories_only, products_without_categories
