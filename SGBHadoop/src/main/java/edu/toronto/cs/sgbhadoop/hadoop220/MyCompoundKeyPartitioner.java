package edu.toronto.cs.sgbhadoop.hadoop220;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class MyCompoundKeyPartitioner extends Partitioner<MyCompoundKey, Text> {
	@Override
	public int getPartition(MyCompoundKey arg0, Text arg1, int arg2) {
		final int hash = arg0.s.hashCode();
		final int partition = (hash & Integer.MAX_VALUE) % arg2;
		return partition;
	}
	
		
	public static int getPartitionStatic(String key, int numPartitions) {
		final int hash = key.hashCode();
		final int partition = (hash & Integer.MAX_VALUE) % numPartitions;
		return partition;
	}
}