package edu.toronto.cs.sgbhadoop.hadoop220;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class MyCompoundKeyComparator extends WritableComparator {
	protected MyCompoundKeyComparator() {
		super(MyCompoundKey.class, true);
	}

	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		final MyCompoundKey key1 = (MyCompoundKey) a;
		final MyCompoundKey key2 = (MyCompoundKey) b;
		final int result = key1.s.compareTo(key2.s);
		if (result == 0) {
			return key1.order - key2.order;
			//			result = (key1.order < key2.order) ? -1 : result;
			//			result = (key1.order == key2.order) ? 0 : result;
			//			result = (key1.order > key2.order) ? 1 : result;
		}
		return result;
	}
}