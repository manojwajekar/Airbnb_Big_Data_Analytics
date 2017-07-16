# Databricks notebook source
# Creating dataframe from database table.
data = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").option("parserLib", "UNIVOCITY").option("wholeFile","true").load("/FileStore/tables/hu67viq41500235640093/reviews.csv")

# COMMAND ----------

# show sample
display(data.take(50))

# COMMAND ----------



# COMMAND ----------

data.filter(data.listing_id)

# COMMAND ----------

from pyspark.sql.types import BooleanType
import re
def regex_filter(x):
    regexs = ['\d+']
    
    if x and x.strip():
        for r in regexs:
            if re.match(r, x, re.IGNORECASE):
                return True
    
    return False 
    
    
filter_udf = udf(regex_filter, BooleanType())

data_filter = data.filter(filter_udf(data.listing_id))

# COMMAND ----------

display(data_filter.take(500))

# COMMAND ----------


