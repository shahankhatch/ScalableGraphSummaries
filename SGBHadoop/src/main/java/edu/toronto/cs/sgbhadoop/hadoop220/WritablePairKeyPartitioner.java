package edu.toronto.cs.sgbhadoop.hadoop220;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class WritablePairKeyPartitioner extends Partitioner<WritablePair, Text> {
	@Override
	public int getPartition(WritablePair arg0, Text arg1, int arg2) {
		final int hash = arg0.a.hashCode();
		final int partition = hash & Integer.MAX_VALUE % arg2;
		return partition;
	}
}