package u1525150;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class IntPairSortComparator extends WritableComparator {
	public IntPairSortComparator() {
		super(IntPair.class);
	}
	
	public int compare(WritableComparable<IntPair> key1, WritableComparable<IntPair> key2) {
		
		return 0;
	}
}
