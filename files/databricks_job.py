from pyspark.sql.types import *
from databricks.connect import DatabricksSession


spark = DatabricksSession.builder.getOrCreate()

line_item  = spark.read.table("gshen_catalog.tpch.lineitem")
line_item.show()