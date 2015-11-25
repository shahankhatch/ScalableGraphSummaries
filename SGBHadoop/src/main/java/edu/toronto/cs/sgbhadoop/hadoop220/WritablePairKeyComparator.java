package edu.toronto.cs.sgbhadoop.hadoop220;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class WritablePairKeyComparator extends WritableComparator {
	protected WritablePairKeyComparator() {
		super(WritablePair.class, true);
	}

	@Override
	public int compare(WritableComparable wc1, WritableComparable wc2) {
		final WritablePair key1 = (WritablePair) wc1;
		final WritablePair key2 = (WritablePair) wc2;
		return key1.compareTo(key2);
	}
}