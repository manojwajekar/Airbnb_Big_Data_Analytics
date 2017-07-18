# Databricks notebook source
import pyspark.sql.functions as func
from pyspark.sql.functions import split
from pyspark.sql.functions import rand,when
from pyspark.ml.feature import CountVectorizer 

# COMMAND ----------

# Creating dataframe from database table.
data = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").option("parserLib", "UNIVOCITY").option("wholeFile","true").load("/FileStore/tables/hu67viq41500235640093/reviews.csv")

# COMMAND ----------

data = data.limit(500)

# COMMAND ----------

# show sample
display(data.take(50))

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

data_filter = data_filter.withColumn('target', when(rand() > 0.5, 1).otherwise(0))

from pyspark.sql.functions import col

data_filter_group_by = data_filter.groupby('target').agg(func.concat_ws(", ",func.collect_list(data_filter.comments))).withColumnRenamed("concat_ws(, , collect_list(comments))","comments")

#data_filter_sample = data_filter.where(col("listing_id").isin(["7543649","1046093","100184","12412900","2635078","13486730","6602421","16031982","17494091","7330060"]))

#data_filter_sample =data_filter.head(500).toDF();
#display(data_filter_sample);

display(data_filter_group_by.take(500))

# COMMAND ----------

import nltk
from nltk.corpus import stopwords 
nltk.download('stopwords')
# function to get the non trivial words from the string
stop=stopwords.words('english') 
def purify(str_line):
    str_line =re.sub('[^\w\s]+', ' ', str_line) 
    str_line =re.sub('\s+', ' ', str_line)
    str_line =re.sub('\d+', ' ', str_line)
    str_line = re.sub("'", '', str_line)
    str_line =re.sub('(\\b[A-Za-z] \\b|\\b [A-Za-z]\\b)', '', str_line)
    str_line = str_line.lower()
    str_words = [ j for j in str_line.split() if j not in stop]
    return str_words

# COMMAND ----------

#pos_word_list = data_filter_group_by.where(col("target") == 1)

pos_word_list = str(data_filter_group_by.where(col("target") == 1).collect())
pos_word_list = purify(pos_word_list)

neg_word_list = str(data_filter_group_by.where(col("target") == 0).collect())
neg_word_list = purify(neg_word_list)

# COMMAND ----------

pos_word_list

# COMMAND ----------

pos_words = sc.parallelize(pos_word_list)
neg_words = sc.parallelize(neg_word_list)

# COMMAND ----------

pos_counts = pos_words.map(lambda x: (x,1)).reduceByKey(lambda a,b: a+b)
neg_counts = neg_words.map(lambda x: (x,1)).reduceByKey(lambda a,b: a+b)

# COMMAND ----------

pos_reversed_map = pos_counts.map(lambda (k,v): (v,k)).sortByKey(False)
pos_original_map = pos_reversed_map.map(lambda (k,v): (v,k))
pos_original_map.take(10)

# COMMAND ----------

neg_reversed_map = neg_counts.map(lambda (k,v): (v,k)).sortByKey(False)
neg_original_map = neg_reversed_map.map(lambda (k,v): (v,k))
neg_original_map.take(10)

# COMMAND ----------




#data_filter_sample.groupBy("listing_id").agg("listing_id","comments")

#data_filter_sample_group_by = data_filter.groupby("listing_id").agg(func.concat_ws(", ",func.collect_list(data_filter.comments))).withColumnRenamed("concat_ws(, , collect_list(comments))","comments")


#data_filter_sample_group_by.columns =["listing_id","comments"]

#data_filter_sample_group_by.schema.names

#display(data_filter_sample_group_by)

# COMMAND ----------

#data_filter_sample_group_by(col("comments")) = data_filter_sample_group_by(col("comments").split(" "))

#df['comments'].str.lower()

from pyspark.sql.functions import split

data_filter_sample_group_by = data_filter_sample_group_by.withColumn("comments", split("comments", "\s+"))

display(data_filter_sample_group_by)


# COMMAND ----------

from pyspark.ml.feature import CountVectorizer

cv = CountVectorizer(inputCol="comments", outputCol="features", vocabSize=50, minDF=2.0)

model = cv.fit(data_filter_sample_group_by)

model_data = model.transform(data_filter_sample_group_by)
#result.show(truncate=False)

model.vocabulary


# COMMAND ----------

# Sample code for CountVectorizer

from pyspark.ml.feature import CountVectorizer

# Input data: Each row is a bag of words with a ID.
df = spark.createDataFrame([
    (0, "a b c".split(" ")),
    (1, "a b b c a".split(" "))
], ["id", "words"])

#display(df)

#fit a CountVectorizerModel from the corpus.
cv = CountVectorizer(inputCol="words", outputCol="features", vocabSize=3, minDF=2.0)

model = cv.fit(df)

result = model.transform(df)
display(result)

# COMMAND ----------

from pyspark.sql.functions import rand,when
model_data = model_data.withColumn('target', when(rand() > 0.5, 1).otherwise(0))
display(model_data)

# COMMAND ----------

#model_data_test = model_data.where(col("listing_id") > 9263729)
#model_data_train = model_data.where(col("listing_id") < 9263729)

model_data_group_by = model_data.groupby("target").agg(func.concat_ws(", ",func.collect_list(model_data.comments)))   #.withColumnRenamed("concat_ws(, , collect_list(comments))","comments")

# COMMAND ----------

display(model_data_test)

# COMMAND ----------


