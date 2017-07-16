--Import piggybank
REGISTER /home/cloudera/Desktop/pig/trunk/contrib/piggybank/java/piggybank.jar;

--Loading data
data = LOAD 'reviews_b.csv' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',') AS (listing_id:int,id:int, date:chararray, reviewer_id:int,reviewer_name:chararray, comments:chararray);

--Filtering data
data_filter = FILTER data BY comments is not NULL;

--Cleaning data 
data_clean = FOREACH data_filter GENERATE listing_id,id,date,TOKENIZE(REPLACE(LOWER(comments),'([^a-zA-Z]+)',' '));

--Storing clean file to HDFS
STORE data_clean INTO 'reviews_clean.csv'; 

