package hw1.job1.mapreduce;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.StringTokenizer;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

import com.opencsv.RFC4180Parser;

import hw1.utils.ColumnIndexes;

public class Job1Mapper extends Mapper<LongWritable, Text, IntWritable, Text> {

	private String tokens = "[_|$#<>\\^=\\[\\]\\*/\\\\,;,.\\-:()?!\"']";
	private IntWritable year = new IntWritable();
	private Text word = new Text();
	
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		if(key.get() == 0) {
			return;
		}
		
		String line = value.toString();
		RFC4180Parser parser = new RFC4180Parser();
		String[] fields = parser.parseLine(line);
		Date time = new Date((long)Long.valueOf(fields[ColumnIndexes.TIME])*1000L);
		SimpleDateFormat format = new SimpleDateFormat("yyyy");
		String yearString = format.format(time);
		year.set(Integer.valueOf(yearString));
		
		String cleanSummary = fields[ColumnIndexes.SUMMARY].toLowerCase().replaceAll(tokens, " ");
		StringTokenizer tokenizer = new StringTokenizer(cleanSummary);
		while(tokenizer.hasMoreTokens()) {
			word.set(tokenizer.nextToken().trim());
			context.write(year, word);
		}
		
	}

}
