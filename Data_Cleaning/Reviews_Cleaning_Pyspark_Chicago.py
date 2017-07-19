############################################################
# Text Mining for reviews using Pyspark
# Author: Manoj Wajekar
############################################################

# Import required packages
import pyspark.sql.functions as func
from pyspark.sql.functions import split
from pyspark.sql.functions import rand,when
from pyspark.ml.feature import CountVectorizer
from pyspark.sql.functions import col
import nltk
from nltk.corpus import stopwords 
nltk.download('stopwords')

# Creating dataframe from csv file.
data = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").option("parserLib", "UNIVOCITY").option("wholeFile","true").load("/FileStore/tables/th776k3r1500350625365/reviews_chicago.csv")

# To remove listing ID which doesnt start with number.
# TODO: It doesnt work for some rows, Need to check logic.

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

# Create separate dataframe based on the outliers in room pricing 
# data_filter_pos --> greater than average price
# data_filter_neg --> lower than average price
data_filter_pos = data_filter.where(col("listing_id").isin(["7921556","18479564","10452642","14859885","6794333","12382366","3629096","16031982","17494091","7330060"]))

data_filter_neg = data_filter.where(col("listing_id").isin(["8459072","1027405","17565878","7124097","15757858","7360828","7327846","14054966","14055052",  "17679124","7796730","13186084"]))


# Concat all the reviews for single row
data_filter_pos_group_by = data_filter_pos.agg(func.concat_ws(", ",func.collect_list(data_filter_pos.comments))).withColumnRenamed("concat_ws(, , collect_list(comments))","comments")

data_filter_neg_group_by = data_filter_neg.agg(func.concat_ws(", ",func.collect_list(data_filter_neg.comments))).withColumnRenamed("concat_ws(, , collect_list(comments))","comments")

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

# Clean the string
pos_word_list = str(data_filter_pos_group_by.collect())
pos_word_list = purify(pos_word_list)

neg_word_list = str(data_filter_neg_group_by.collect())
neg_word_list = purify(neg_word_list)

pos_words = sc.parallelize(pos_word_list)
neg_words = sc.parallelize(neg_word_list)

# Creating Bag of Words
pos_counts = pos_words.map(lambda x: (x,1)).reduceByKey(lambda a,b: a+b)
neg_counts = neg_words.map(lambda x: (x,1)).reduceByKey(lambda a,b: a+b)

# Positive words
pos_reversed_map = pos_counts.map(lambda (k,v): (v,k)).sortByKey(False)
pos_original_map = pos_reversed_map.map(lambda (k,v): (v,k))
pos_original_map.take(10)

# Negative words
neg_reversed_map = neg_counts.map(lambda (k,v): (v,k)).sortByKey(False)
neg_original_map = neg_reversed_map.map(lambda (k,v): (v,k))
neg_original_map.take(10)
