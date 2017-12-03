package u1525150;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class IntPairSortComparator extends WritableComparator {
	public IntPairSortComparator() {
		super(IntPair.class, true);
	}
	
	@SuppressWarnings("rawtypes")
	@Override
	public int compare(WritableComparable key1, WritableComparable key2) {
		if (key1 instanceof IntPair && key2 instanceof IntPair) {
			return ((IntPair) key1).compareTo((IntPair) key2);
		}
		return super.compare(key1, key2);
	}
}
