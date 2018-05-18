package hw1.spark;

import java.util.HashSet;
import java.util.Set;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.opencsv.RFC4180Parser;

import hw1.utils.ColumnIndexes;
import scala.Tuple2;

public class Job3Spark {

	private static String inputPathToFile;
	private static String outputPathFile;

	public Job3Spark(String inputPath, String outputPath) {
		Job3Spark.inputPathToFile = inputPath;
		Job3Spark.outputPathFile = outputPath;
	}

	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("Job3Spark");
		JavaSparkContext sc = new JavaSparkContext(conf);

		if(args.length < 2) {
			System.err.println("Usage: <input-file> <output-folder>");
		}

		Job3Spark job = new Job3Spark(args[0], args[1]);
		job.coupleProducts2Users(sc).sortByKey().saveAsTextFile(outputPathFile);
		sc.close();
		sc.stop();
	}

	public JavaPairRDD<String, String> loadData(JavaSparkContext sc) {
		JavaRDD<String> words = sc.textFile(inputPathToFile);
		JavaPairRDD<String, String> result = 
				words.mapPartitionsWithIndex((idx, iter) -> {
					if (idx == 0 && iter.hasNext()) {
						iter.next();
					}
					return iter;
				}, true)
				.mapToPair(line -> {
					RFC4180Parser parser = new RFC4180Parser();
					String[] fields = parser.parseLine(line);
					return new Tuple2<String, String>(fields[ColumnIndexes.USER_ID], fields[ColumnIndexes.PRODUCT_ID]);
				});
		return result;
	}

	public JavaPairRDD<String, Integer> coupleProducts2Users(JavaSparkContext sc) {
		JavaPairRDD<String, String> user2products = loadData(sc);
		JavaPairRDD<String, Integer> result = user2products.join(user2products)
		.filter(user2couple -> user2couple._2._1.compareTo(user2couple._2._2) < 0)
		.mapToPair(elem -> new Tuple2<String, String>(elem._2._1 + ", "+ elem._2._2, elem._1))
		.sortByKey()
		.groupByKey()
		.mapToPair(couple2user -> {
			Set<String> users = new HashSet<>();
			couple2user._2.forEach(users::add);
			return new Tuple2<String, Integer>(couple2user._1, users.size());
		});
		return result;
	}
}
