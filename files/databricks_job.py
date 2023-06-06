from pyspark.sql.types import *

# Read data from Databricks table
line_item = spark.read.table("gshen_catalog.tpch.lineitem")

# Aggregate dataframe by l_supplykey and get the count
line_item_agg = line_item.groupBy("l_suppkey").count()

# Rename dataframe column count to count_num
line_item_agg = line_item_agg.withColumnRenamed("count", "count_num")

# Write data to Databricks table
line_item_agg.write.mode("overwrite").option("overwriteSchema", "True").saveAsTable(
    "gshen_catalog.tpch.lineitem_agg"
)
