package hw1.spark;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.opencsv.RFC4180Parser;

import hw1.utils.ColumnIndexes;
import hw1.utils.MapUtil;
import hw1.utils.UnixTime2Year;
import scala.Tuple2;

public class Job1Spark implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private static String inputPathToFile;
	private static String outputPathFile;
	private static String tokens = "[_|$#<>\\^=\\[\\]\\*/\\\\,;,.\\-:()?!\"']";

	public Job1Spark(String inputPath, String outputPath) {
		Job1Spark.inputPathToFile = inputPath;
		Job1Spark.outputPathFile = outputPath;
	}

	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("Job1Spark");
		JavaSparkContext sc = new JavaSparkContext(conf);

		if(args.length < 2) {
			System.err.println("Usage: <input-file> <output-folder>");
		}

		Job1Spark job = new Job1Spark(args[0], args[1]);
		job.getCount(sc).sortByKey().saveAsTextFile(outputPathFile);
		sc.close();
		sc.stop();
	}

	public JavaPairRDD<Integer, List<String>> loadData(JavaSparkContext sc) {
		JavaRDD<String> words = sc.textFile(inputPathToFile);
		JavaPairRDD<Integer,List<String>> couples = 
				words.mapPartitionsWithIndex((idx, iter) -> {
					if (idx == 0 && iter.hasNext()) {
						iter.next();
					}
					return iter;
				}, true)
				.mapToPair(
						line -> {
							RFC4180Parser parser = new RFC4180Parser();
							String[] fields = parser.parseLine(line);
							String summary = fields[ColumnIndexes.SUMMARY].toLowerCase().replaceAll(tokens, " ");
							return new Tuple2<Integer, List<String>>(UnixTime2Year.unixTime2Year(Long.parseLong(fields[ColumnIndexes.TIME])), 
									Arrays.asList(summary.split(" ")));
						}).mapValues(word -> {
							List<String> trimmed = new ArrayList<String>();
							for(int i = 0; i < word.size(); i++) {
								String w = word.get(i).trim();
								if(!w.isEmpty())
									trimmed.add(w);
							}
							return trimmed;
						}).groupByKey().flatMapValues(x -> x);
		return couples;
	}

	public JavaPairRDD<Integer, List<Tuple2<String, Integer>>> getCount(JavaSparkContext sc) {
		JavaPairRDD<Integer, List<String>> couples = loadData(sc);

		JavaPairRDD<Integer, List<Tuple2<String, Integer>>> result = 
				couples.groupByKey()
				.mapToPair(tuple -> {
					List<String> elements = new ArrayList<>();
					tuple._2.forEach(elements::addAll);
					Map<String, Integer> word2freq = 
							elements.stream()
							.collect(Collectors.groupingBy(Function.identity(), Collectors.summingInt(e -> 1)));
					Map<String, Integer> sorted = MapUtil.sortByValue(word2freq);
					List<Tuple2<String, Integer>> count = new ArrayList<>();
					sorted.forEach((key, value) -> {
						Tuple2<String, Integer> tp = new Tuple2<>(key, value);
						count.add(tp);
					});
					int size = 10;
					if(count.size() < 10)
						size = count.size();

					List<Tuple2<String, Integer>> cutted = new ArrayList<>(count.subList(0, size));

					return new Tuple2<Integer, List<Tuple2<String, Integer>>>(tuple._1, cutted);
				});
		return result;
	}

}
