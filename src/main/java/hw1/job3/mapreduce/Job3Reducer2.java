package hw1.job3.mapreduce;

import java.io.IOException;
import java.util.HashSet;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Job3Reducer2 extends Reducer<Text, Text, Text, IntWritable> {
	
	public void reduce(Text key, Iterable<Text> values, Context context) 
			throws IOException, InterruptedException {
		
		HashSet<String> userIDs = new HashSet<String>();
		
		for(Text userID : values) {
			userIDs.add(userID.toString());
		}
		
		context.write(key, new IntWritable(userIDs.size()));
	}

}
