package hw1.job3.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Job3Mapper2 extends Mapper<LongWritable, Text, Text, Text> {
	
	private Text productCouple = new Text();
	private Text userID = new Text();
	
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String[] fields = value.toString().split("\t");
		
		productCouple.set(fields[0]);
		userID.set(fields[1]);
		
		context.write(productCouple, userID);
	}
}
