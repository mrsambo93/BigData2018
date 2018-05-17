package hw1.job3.mapreduce;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Job3MapReduce {
	public static void main(String[] args) throws Exception {
    	Configuration conf = new Configuration();
    	Path tempFile = new Path("~/output/temp.txt");
		Job job1 = new Job(conf, "Job3MapReduce1");
    	
		job1.setJarByClass(Job3MapReduce.class);
		
		job1.setMapperClass(Job3Mapper1.class);
		job1.setReducerClass(Job3Reducer1.class);

		FileInputFormat.addInputPath(job1, new Path(args[0]));
		FileOutputFormat.setOutputPath(job1, tempFile);

		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(Text.class);
		
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);

		job1.waitForCompletion(true);
		
		Job job2 = new Job(conf, "Job3MapReduce1");
    	
		job2.setJarByClass(Job3MapReduce.class);
		
		job2.setMapperClass(Job3Mapper2.class);
		job2.setReducerClass(Job3Reducer2.class);

		FileInputFormat.addInputPath(job2, tempFile);
		FileOutputFormat.setOutputPath(job2, new Path(args[1]));

		job2.setMapOutputKeyClass(Text.class);
		job2.setMapOutputValueClass(Text.class);
		
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(IntWritable.class);

		job2.waitForCompletion(true);
		
		FileSystem hdfs = FileSystem.get(URI.create("hdfs://localhost:9000"), conf);
		if(hdfs.exists(tempFile))
			hdfs.delete(tempFile, true);

    }
}
