package u1525150;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

public class IntPair implements WritableComparable<IntPair> {

	private IntWritable firstInt;
	private IntWritable secondInt;
	
	public IntPair() {
	}
	
	public IntPair(int firstInt, int secondInt) {
		set(new IntWritable(firstInt), new IntWritable(secondInt));
	}
	
	public IntPair(IntWritable firstInt, IntWritable secondInt) {
		set(firstInt, secondInt);
	}
	
	public void set(IntWritable first, IntWritable second) {
		firstInt = first;
		secondInt = second;
	}
	
	public IntWritable getFirst() {
		return firstInt;
	}
	
	public IntWritable getSecond() {
		return secondInt;
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		firstInt.write(out);	
		secondInt.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		firstInt = new IntWritable(in.readInt());
		secondInt = new IntWritable(in.readInt());
	}
	
	@Override
	public int compareTo(IntPair otherPair) {
		//sort count in reverse order and then id in natural order
		if (!firstInt.equals(otherPair.getFirst())) {
			//sorts in descending order by reversing natural order
			return firstInt.compareTo(otherPair.getFirst()) * -1; 
		} else {
			// if the firstInt's are equal, sort by secondInt in ascending order
			return secondInt.compareTo(otherPair.getSecond()); 
		}
	}
	
	@Override
	public int hashCode() {
		return firstInt.hashCode() + secondInt.hashCode();
	}
	
	@Override
	public boolean equals(Object otherPair) {
		if (otherPair instanceof IntPair) {
			return firstInt.equals(((IntPair)otherPair).getFirst()) && secondInt.equals(((IntPair) otherPair).getSecond());
		}
		return false; 
	}
	
	@Override
	public String toString() {
		//for TextOutputFormat compliance
		return firstInt.toString() + "\t" + secondInt.toString();
	}
}
