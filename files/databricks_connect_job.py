from pyspark.sql.types import *
from databricks.connect import DatabricksSession


spark = DatabricksSession.builder.getOrCreate()

# Read data from Databricks table
line_item = spark.read.table("gshen_catalog.tpch.lineitem")

# Aggregate dataframe by l_supplykey and get the count
line_item_agg = line_item.groupBy("l_suppkey").count()

# Write data to Databricks table
line_item_agg.write.mode("overwrite").option("overwriteSchema", "True").saveAsTable(
    "gshen_catalog.tpch.lineitem_agg_db_connect"
)
