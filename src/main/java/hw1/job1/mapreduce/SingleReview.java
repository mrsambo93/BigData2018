package hw1.job1.mapreduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;


public class SingleReview implements WritableComparable<SingleReview> {
	private Text summary;
	private IntWritable frequency;
	
	public SingleReview(Text summary, IntWritable frequency) {
		this.summary = summary;
		this.frequency = frequency;
	}

	public Text getSummary() {
		return summary;
	}

	public void setSummary(Text summary) {
		this.summary = summary;
	}

	public IntWritable getFrequency() {
		return frequency;
	}

	public void setFrequency(IntWritable frequency) {
		this.frequency = frequency;
	}
	
	public int compareTo(SingleReview r) {
		return this.getFrequency().compareTo(r.getFrequency());
	}

	public void readFields(DataInput arg0) throws IOException {
		summary = new Text(arg0.readUTF());
		frequency = new IntWritable(arg0.readInt());
	}

	public void write(DataOutput arg0) throws IOException {
		arg0.writeUTF(summary.toString());
		arg0.writeInt(frequency.get());
	}
	
	@Override
	public String toString() {
		return this.getSummary().toString() + " " + this.getFrequency().get();
	}

}
