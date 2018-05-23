CREATE TABLE IF NOT EXISTS amazonfoodreviews
(id INT, productId STRING, userId STRING, profileName STRING, helpfulnessNumerator INT,
helpfulnessDenominator INT, score INT, time BIGINT, summary STRING, text STRING)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
with serdeproperties (
   "separatorChar" = ",",
   "quoteChar"     = "\""
  )
TBLPROPERTIES("skip.header.line.count"="1");

LOAD DATA LOCAL INPATH "/home/mrsambo/Documenti/BigData/Homework1/Reviews.csv"
OVERWRITE INTO TABLE amazonfoodreviews;

add jar /home/mrsambo/Documenti/BigData/Homework1/u2d-unix_date.jar;

CREATE TEMPORARY FUNCTION unix_date AS 'u2d.u2d.Unix2Date';

CREATE OR REPLACE VIEW product_year AS
SELECT productID, unix_date(time) AS year, score
FROM amazonfoodreviews;

SELECT productID, year, avg(score) AS scr
FROM product_year
WHERE year >= 2003 AND year <= 2012
GROUP BY productID, year;