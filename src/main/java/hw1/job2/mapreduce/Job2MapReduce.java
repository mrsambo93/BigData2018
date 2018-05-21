package hw1.job2.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Job2MapReduce {
	public static void main(String[] args) throws Exception {
		double startTime = System.currentTimeMillis();
		
    	Job job = new Job(new Configuration(), "Job2MapReduce");

		job.setJarByClass(Job2MapReduce.class);
		
		job.setMapperClass(Job2Mapper.class);
		job.setReducerClass(Job2Reducer.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Year2Score.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.waitForCompletion(true);
		
		double stopTime = System.currentTimeMillis();
		double executionTime = (stopTime - startTime) / 1000;
		System.out.println("TEMPO DI ESECUZIONE:\t" + executionTime + "s");
    }

}
