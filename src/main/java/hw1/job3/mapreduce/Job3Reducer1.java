package hw1.job3.mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Job3Reducer1 extends Reducer<Text, Text, Text, Text> {
	
	public void reduce(Text key, Iterable<Text> values, Context context) 
			throws IOException, InterruptedException {
		
		HashSet<String> productsSet = new HashSet<String>();
		
		for(Text prod : values) {
			productsSet.add(prod.toString());
		}
		
		ArrayList<String> products = new ArrayList<String>(productsSet);
		
		for(int i = 0; i < products.size() - 1; i++) {
			String product1 = products.get(i);
			for(int j = i + 1; j < products.size(); j++) {
				String product2 = products.get(j);
				String couple = "<" + product1 + "," + product2 + ">";
				context.write(new Text(couple), key);
			}
		}
	}
}
