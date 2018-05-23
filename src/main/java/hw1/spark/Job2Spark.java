package hw1.spark;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.opencsv.RFC4180Parser;

import hw1.utils.ColumnIndexes;
import hw1.utils.UnixTime2Year;
import scala.Tuple2;

public class Job2Spark {

	private static String inputPathToFile;
	private static String outputPathFile;

	public Job2Spark(String inputPath, String outputPath) {
		Job2Spark.inputPathToFile = inputPath;
		Job2Spark.outputPathFile = outputPath;
	}

	public static void main(String[] args) {
		double startTime = System.currentTimeMillis();
		
		SparkConf conf = new SparkConf().setAppName("Job2Spark");
		JavaSparkContext sc = new JavaSparkContext(conf);

		if(args.length < 2) {
			System.err.println("Usage: <input-file> <output-folder>");
		}

		Job2Spark job = new Job2Spark(args[0], args[1]);
		job.calculateScore(sc).sortByKey().saveAsTextFile(outputPathFile);
		sc.close();
		sc.stop();

		double stopTime = System.currentTimeMillis();
		double executionTime = (stopTime - startTime) / 1000;
		System.out.println("TEMPO DI ESECUZIONE:\t" + executionTime + "s");
	}

	public JavaPairRDD<String, Tuple2<Integer, Double>> loadData(JavaSparkContext sc) {
		JavaRDD<String> words = sc.textFile(inputPathToFile);
		JavaPairRDD<String, Tuple2<Integer, Double>> result = 
				words.mapPartitionsWithIndex((idx, iter) -> {
					if (idx == 0 && iter.hasNext()) {
						iter.next();
					}
					return iter;
				}, true)
				.mapToPair(line -> {
					RFC4180Parser parser = new RFC4180Parser();
					String[] fields = parser.parseLine(line);
					int year = UnixTime2Year.unixTime2Year(Long.parseLong(fields[ColumnIndexes.TIME]));
					return new Tuple2<String, Tuple2<Integer, Double>>(fields[ColumnIndexes.PRODUCT_ID],
							new Tuple2<Integer, Double>(year, Double.parseDouble(fields[ColumnIndexes.SCORE])));
				});
		return result;
	}

	public JavaPairRDD<String, List<Tuple2<Integer, Double>>> calculateScore(JavaSparkContext sc) {
		JavaPairRDD<String, Tuple2<Integer, Double>> couples = loadData(sc);
		JavaPairRDD<String, List<Tuple2<Integer, Double>>> result = couples
				.groupByKey()
				.mapToPair(tuple -> {
					List<Tuple2<Integer, Double>> elements = new ArrayList<>();
					tuple._2.forEach(elements::add);
					Map<Integer, Double> avgScoresPerYear =  
							elements.stream()
							.filter(t -> t._1 >= 2003 && t._1 <= 2012)
							.collect(Collectors.groupingBy(e -> e._1, Collectors.averagingDouble(e -> e._2)));

					List<Tuple2<Integer, Double>> count = new ArrayList<>();
					avgScoresPerYear.forEach((key, value) -> {
						Tuple2<Integer, Double> tp = new Tuple2<>(key, value);
						count.add(tp);
					});
					return new Tuple2<String, List<Tuple2<Integer, Double>>>(tuple._1, count);
				});
		return result.filter(el -> !el._2.isEmpty());
	}
}
