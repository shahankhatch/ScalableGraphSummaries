package edu.toronto.cs.partition;

import java.util.HashMap;
import java.util.TreeSet;

import org.apache.commons.codec.digest.DigestUtils;

public class PartitionInstanceChecker {

	// actual functionality provided elsewhere, need to refactor and bring it into this class

	// what does this class do?
	// compares an instance with its block peers
	// if its peers are the same between iterations
	// then the block was not refined
	// if the signature of the peers is different, then the block has been refined
	// where is the instance, block, partition, iteration

	// map from partition id to ordered set of instances
	public HashMap<String, TreeSet<Integer>> partition1 = new HashMap<String, TreeSet<Integer>>();
	public HashMap<String, TreeSet<Integer>> partition2 = new HashMap<String, TreeSet<Integer>>();

	// map from partition id to hash of its instances
	public HashMap<String, String> partition1InstanceSetHash = new HashMap<String, String>();
	public HashMap<String, String> partition2InstanceSetHash = new HashMap<String, String>();

	public void computeInstanceSetHashes() {
		for (String k : partition2.keySet()) {
			TreeSet<Integer> v = partition2.get(k);
			partition2InstanceSetHash.put(k, DigestUtils.md5Hex(v.toString()));
		}
	}

	// functionality:
	// move partition2 to partition1 after it has been compared?
	// compare instance set hashes of two partitions, sort instances lexicographically by their (blockid, nodeid), compute instance hash for each distinct blockid  

}
