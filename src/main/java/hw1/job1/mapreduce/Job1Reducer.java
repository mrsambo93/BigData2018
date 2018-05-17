package hw1.job1.mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Job1Reducer extends Reducer<IntWritable, Text, IntWritable, Text> {

	public void reduce(IntWritable key, Iterable<Text> values, Context context) 
			throws IOException, InterruptedException {

		HashMap<String, Integer> summary2frequency = new HashMap<String, Integer>();

		for(Text value : values) {
			String summary = value.toString();
			Integer frequency = 1;
			if(summary2frequency.containsKey(summary)) {
				frequency = summary2frequency.get(summary) + 1;
			}
			summary2frequency.put(summary, frequency);
		}
		
		ArrayList<SingleReview> reviews = new ArrayList<SingleReview>();
		
		for(String word : summary2frequency.keySet()) {
			Text text = new Text();
			text.set(word);
			IntWritable freq = new IntWritable();
			freq.set(summary2frequency.get(word));
			
			SingleReview rev = new SingleReview(text, freq);
			reviews.add(rev);
		}
		
		Collections.sort(reviews);
		Collections.reverse(reviews);
		
		int len = reviews.size();
		
		if(len > 10)
			len = 10;
		
		List<SingleReview> cutted = reviews.subList(0, len);
		
		StringBuilder val = new StringBuilder();
		
		for(int i = 0; i < cutted.size(); i++) {
			SingleReview rev = cutted.get(i);
			val.append(rev.toString());
			if(i < cutted.size() - 1)
				val.append(", ");
		}
		
		Text output = new Text();
		output.set(val.toString());
		
		context.write(key, output);

	}
}
