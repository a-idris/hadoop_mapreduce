package u1525150;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class IntPair implements WritableComparable<IntPair> {

	private int firstInt;
	private int secondInt;
	
	public IntPair() {
	}
	
	public IntPair(int firstInt, int secondInt) {
		set(firstInt, secondInt);
	}
	
	public void set(int first, int second) {
		firstInt = first;
		secondInt = second;
	}
	
	public int getFirst() {
		return firstInt;
	}
	
	public int getSecond() {
		return secondInt;
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(firstInt);
		out.writeInt(secondInt);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		firstInt = in.readInt();
		secondInt = in.readInt();
	}
	
	@Override
	public int compareTo(IntPair otherPair) {
		//sort count in reverse order and id in natural order
		if (getFirst() != otherPair.getFirst()) {
			return Integer.compare(getFirst(), otherPair.getFirst()) * -1;
		} else {
			return Integer.compare(getSecond(), otherPair.getSecond()); // SKILL LEVEL?
		}
	}
	
	public String toString() {
		return firstInt + "\t" + secondInt;
	}
}
