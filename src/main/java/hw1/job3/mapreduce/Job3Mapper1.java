package hw1.job3.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.opencsv.RFC4180Parser;

import hw1.utils.ColumnIndexes;

public class Job3Mapper1 extends Mapper<LongWritable, Text, Text, Text> {
	
	private Text productID = new Text();
	private Text userID = new Text();
	
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		
		if(key.get() == 0) {
			return;
		}
		
		String line = value.toString();
		RFC4180Parser parser = new RFC4180Parser();
		String[] fields = parser.parseLine(line);
		
		productID.set(fields[ColumnIndexes.PRODUCT_ID]);
		userID.set(fields[ColumnIndexes.USER_ID]);
		
		context.write(userID, productID);
	}
	
}
