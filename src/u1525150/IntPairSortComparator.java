package u1525150;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class IntPairSortComparator extends WritableComparator {
	public IntPairSortComparator() {
		super(IntPair.class, true);
	}
	
//	@Override
//	public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
//		return 0;
//	}
	
	@Override
	public int compare(WritableComparable key1, WritableComparable key2) {
		if (key1 instanceof IntPair && key2 instanceof IntPair) {
			return ((IntPair) key1).compareTo((IntPair) key2);
		}
		return super.compare(key1, key2);
	}
}
