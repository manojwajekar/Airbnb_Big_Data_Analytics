# Databricks notebook source
# Import required packages

import pyspark.sql.functions as func
from pyspark.sql.functions import struct
from pyspark.sql.functions import split
from pyspark.sql.functions import rand,when
from pyspark.ml.feature import CountVectorizer
from pyspark.sql.functions import col
from pyspark.sql.functions import udf
import nltk
from pyspark.ml.clustering import LDA
from nltk.corpus import stopwords 
from pyspark.sql.types import BooleanType,ArrayType,StringType,StructType,IntegerType
import re
from pyspark.ml.feature import HashingTF, IDF, Tokenizer
#nltk.download('stopwords')

# COMMAND ----------

# Creating dataframe from csv file.
data = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").option("parserLib", "UNIVOCITY").option("wholeFile","true").load("/FileStore/tables/th776k3r1500350625365/reviews_chicago.csv")
display(data)

# COMMAND ----------

# To remove listing ID which doesnt start with number.


# TODO: Something doesnt work, Need to check logic.
def regex_filter(x):
    regexs = ['\d+']
    
    if x and x.strip():
        for r in regexs:
            if re.match(r, x, re.IGNORECASE):
                return True
    return False 
    
    
    
filter_udf = udf(regex_filter, BooleanType())

rawdata = data.filter(filter_udf(data.listing_id))
#rawdata = rawdata.limit(1000)
rawdata = rawdata.filter(rawdata.comments != "")
#display(rawdata)
rawdata.count()

# COMMAND ----------

def cleanup_text(record):
    text  = record[5]
    id   = record[2]
    words = text.split()
    # Default list of Stopwords
    stopwords_core = ['a', u'about', u'above', u'after', u'again', u'against', u'all', u'am', u'an', u'and', u'any', u'are', u'arent', u'as', u'at', 
    u'be', u'because', u'been', u'before', u'being', u'below', u'between', u'both', u'but', u'by', 
    u'can', 'cant', 'come', u'could', 'couldnt', 
    u'd', u'did', u'didn', u'do', u'does', u'doesnt', u'doing', u'dont', u'down', u'during', 
    u'each', 
    u'few', 'finally', u'for', u'from', u'further', 
    u'had', u'hadnt', u'has', u'hasnt', u'have', u'havent', u'having', u'he', u'her', u'here', u'hers', u'herself', u'him', u'himself', u'his', u'how', 
    u'i', u'if', u'in', u'into', u'is', u'isnt', u'it', u'its', u'itself', 
    u'just', 
    u'll', 
    u'm', u'me', u'might', u'more', u'most', u'must', u'my', u'myself', 
    u'no', u'nor', u'not', u'now', 
    u'o', u'of', u'off', u'on', u'once', u'only', u'or', u'other', u'our', u'ours', u'ourselves', u'out', u'over', u'own', 
    u'r', u're', 
    u's', 'said', u'same', u'she', u'should', u'shouldnt', u'so', u'some', u'such', 
    u't', u'than', u'that', 'thats', u'the', u'their', u'theirs', u'them', u'themselves', u'then', u'there', u'these', u'they', u'this', u'those', u'through', u'to', u'too', 
    u'under', u'until', u'up', 
    u'very', 
    u'was', u'wasnt', u'we', u'were', u'werent', u'what', u'when', u'where', u'which', u'while', u'who', u'whom', u'why', u'will', u'with', u'wont', u'would', 
    u'y', u'you', u'your', u'yours', u'yourself', u'yourselves']
    
    # Custom List of Stopwords - Add your own here
    stopwords_custom = [u'automated', u'posting']
    stopwords = stopwords_core + stopwords_custom
    stopwords = [word.lower() for word in stopwords]    
    
    text_out = [re.sub('[^a-zA-Z0-9]','',word) for word in words]                                       # Remove special characters
    text_out = [word.lower() for word in text_out if len(word)>2 and word.lower() not in stopwords]     # Remove stopwords and words under X length
    return text_out

udf_cleantext = udf(cleanup_text , ArrayType(StringType()))
clean_text = rawdata.withColumn("words", udf_cleantext(struct([rawdata[x] for x in rawdata.columns])))

# COMMAND ----------

clean_text['listing_id','words'].write.saveAsTable('clean_words',mode='overwrite')
