CREATE TABLE IF NOT EXISTS amazonfoodreviews
(id INT, productId STRING, userId STRING, profileName STRING, helpfulnessNumerator INT,
helpfulnessDenominator INT, score INT, time BIGINT, summary STRING, text STRING)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
with serdeproperties (
   "separatorChar" = ",",
   "quoteChar"     = "\"",
   "escapeChar"    = "\0"
  )
TBLPROPERTIES("skip.header.line.count"="1");

LOAD DATA LOCAL INPATH "/home/mrsambo/Documenti/BigData/Homework1/Reviews3.csv"
OVERWRITE INTO TABLE amazonfoodreviews;

SELECT
 	t1.productId AS product1,
 	t2.productId AS product2,
 	COUNT(1) AS cnt
FROM
	(
 	SELECT DISTINCT userId, productId
 	FROM amazonfoodreviews
	) t1
JOIN
	(
	SELECT DISTINCT userId, productId
 	FROM amazonfoodreviews
	) t2
ON (t1.userId = t2.userId)
GROUP BY t1.productId, t2.productId
HAVING t1.productId != t2.productId
ORDER BY t1.productId;

