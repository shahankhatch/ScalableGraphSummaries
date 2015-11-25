package edu.toronto.cs.sgbhadoop.hadoop220;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class MyCompoundKeyGroupingComparator extends WritableComparator {
	public MyCompoundKeyGroupingComparator() {
		super(MyCompoundKey.class, true);
	}

	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		final MyCompoundKey key1 = (MyCompoundKey) a;
		final MyCompoundKey key2 = (MyCompoundKey) b;
		return key1.s.compareTo(key2.s);
	}
}