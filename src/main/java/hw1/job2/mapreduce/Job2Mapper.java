package hw1.job2.mapreduce;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import com.opencsv.RFC4180Parser;

import hw1.utils.ColumnIndexes;

public class Job2Mapper extends Mapper<LongWritable, Text, Text, Year2Score> {
	
	private Text productID = new Text();
	private Year2Score year2score = new Year2Score();
	
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
		int year = Integer.valueOf(yearString);
		if(year >= 2003 && year <= 2012) {
			productID.set(fields[ColumnIndexes.PRODUCT_ID]);
			year2score.setYear(year);
			year2score.setScore(Integer.valueOf(fields[ColumnIndexes.SCORE]));
			context.write(productID, year2score);
		}
	}

}
