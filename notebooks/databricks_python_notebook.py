# Databricks notebook source

from pyspark.sql.types import *


line_item  = spark.read.table("gshen_catalog.tpch.lineitem")

# COMMAND ----------

# filter dataframe by column l_suppkey ==11315 and select only l_orderkey
line_item.filter(line_item.l_suppkey == 11315).select("l_orderkey").show()
