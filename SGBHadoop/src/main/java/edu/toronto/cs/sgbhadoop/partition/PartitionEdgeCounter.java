package edu.toronto.cs.sgbhadoop.partition;

import edu.toronto.cs.sgbhadoop.util.FileUtil;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.HashSet;


/** 
 * Count the number of block edges (distinct edges between blocks) in a partition
 * ?? uniquely labeled ?? 
 *
 */
public class PartitionEdgeCounter {
	public static void main(String[] args) {
		if (args.length != 3) {
			System.err.println("args: <inputFileNodes> <inputFileEdges> <outputFile>");
			System.exit(1);
		}
		try {
			new PartitionEdgeCounter(args[0], args[1], args[2]);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	public PartitionEdgeCounter(String inputFileNodes, String inputFileEdges, String outputFile) throws FileNotFoundException, IOException {
		// read node ids
		// read edges
		// collect block edges
		// write distinct block edges

		HashMap<Integer, Integer> instanceBlockMap = new HashMap<Integer, Integer>();
		HashSet<String> blockEdgesSet = new HashSet<String>();

		BufferedReader nodesReader = FileUtil.getBufferedReader(inputFileNodes);
		String line = null;
		while ((line = nodesReader.readLine()) != null) {
			String parse[] = line.split(" ");
			// 0 - nodeid
			// 1 - blockid
			instanceBlockMap.put(Integer.parseInt(parse[0]), Integer.parseInt(parse[1]));
		}
		nodesReader.close();

		line = "";
		BufferedReader edgesReader = FileUtil.getBufferedReader(inputFileEdges);
		int linecounter = 0;
		while ((line = edgesReader.readLine()) != null) {
			if (linecounter++ % 1000000 == 0) {
				System.out.println("line:" + (linecounter - 1));
			}
			String parse[] = line.split(" ");
			blockEdgesSet.add(instanceBlockMap.get(Integer.parseInt(parse[0])) + " " + instanceBlockMap.get(Integer.parseInt(parse[2])));
		}
		edgesReader.close();

		PrintWriter blockedgeWriter = FileUtil.getPrintWriter(outputFile);
		for (String e : blockEdgesSet) {
			blockedgeWriter.write(e + "\n");
		}
		blockedgeWriter.close();

		System.out.println("blockedges:" + blockEdgesSet.size());
	}

}
