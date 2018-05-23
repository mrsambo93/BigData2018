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

CREATE OR REPLACE VIEW cleaned AS
SELECT unix_date(time) AS year, regexp_replace(regexp_replace(lower(summary), "[^a-zA-Z0-9]", " "), "[ ]+","#") AS clean
FROM amazonfoodreviews;

CREATE OR REPLACE VIEW w AS
SELECT year, exp.word
FROM cleaned
LATERAL VIEW explode(split(clean, '#')) exp AS word
WHERE exp.word REGEXP "[^ ]+";

SELECT year, word, word_count
FROM (
	SELECT year, word, count(1) as word_count,
		   row_number() over (partition by year order by count(1) desc) AS row_num
	FROM w
	GROUP BY year, word
) T
WHERE row_num <= 10;