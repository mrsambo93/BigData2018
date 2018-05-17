package hw1.job2.mapreduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

public class Year2Score implements Writable{
	private IntWritable year;
	private IntWritable score;
	private DoubleWritable mean;
	
	public Year2Score() {
		this.year = new IntWritable();
		this.score = new IntWritable();
		this.mean = new DoubleWritable(0.0);
	}
	
	public Year2Score(IntWritable year, IntWritable score) {
		this.year = year;
		this.score = score;
		this.mean = new DoubleWritable(0.0);
	}

	public IntWritable getYear() {
		return year;
	}

	public void setYear(int year) {
		this.year.set(year);
	}

	public IntWritable getScore() {
		return score;
	}

	public void setScore(int score) {
		this.score.set(score);
	}
	
	public DoubleWritable getMean() {
		return this.mean;
	}
	
	public void setMean(double mean) {
		this.mean.set(mean);
	}
 
	public void readFields(DataInput arg0) throws IOException {
		year = new IntWritable(arg0.readInt());
		score = new IntWritable(arg0.readInt());
		mean = new DoubleWritable(arg0.readDouble());
	}

	public void write(DataOutput arg0) throws IOException {
		arg0.writeInt(year.get());
		arg0.writeInt(score.get());
		arg0.writeDouble(mean.get());
	}
	
	@Override
	public String toString() {
		return this.getYear().get() + ": " + this.getMean().get();
	}

}
