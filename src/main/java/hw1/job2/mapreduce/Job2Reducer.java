package hw1.job2.mapreduce;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Job2Reducer extends Reducer<Text, Year2Score, Text, Text> {

	public void reduce(Text key, Iterable<Year2Score> values, Context context) 
			throws IOException, InterruptedException {
		ArrayList<Year2Score> elements = new ArrayList<Year2Score>();
		for(Year2Score y2s : values) {
			Year2Score copy = new Year2Score(y2s.getYear(), y2s.getScore());
			elements.add(copy);
		}

		ArrayList<Year2Score> results = new ArrayList<Year2Score>();
		
		for(int i = 2003; i < 2013; i++) {
			int n = 0;
			int score = 0;
			for(Year2Score year2score : elements) {
				if(year2score.getYear().get() == i) {
					score += year2score.getScore().get();
					n++;
				}
			}
			double mean = 0.0;
			if(n != 0) {
				mean = (double)score/(double)n;
				Year2Score newElem = new Year2Score();
				newElem.setYear(i);
				newElem.setMean(mean);
				results.add(newElem);
			}
		}
		
		if(results.isEmpty())
			return;
		StringBuilder builder = new StringBuilder();
		
		for(int i = 0; i < results.size(); i++) {
			Year2Score el = results.get(i);
			builder.append(el.toString());
			if(i < results.size() - 1)
				builder.append(", ");
		}
		
		Text value = new Text();
		value.set(builder.toString());
		
		context.write(key, value);
	}

}
