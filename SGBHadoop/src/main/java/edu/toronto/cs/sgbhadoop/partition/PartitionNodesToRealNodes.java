package edu.toronto.cs.sgbhadoop.partition;

import edu.toronto.cs.sgbhadoop.util.FileUtil;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;


/**
 * Converts intednodePartitionFile to partition of uris in file named same as intednodePartitionfile and ending '-realuri.gz'
 *
 */
public class PartitionNodesToRealNodes {

	public static void main(String[] args) {
		if (args.length != 2) {
			System.err.println("args: <nodeFileReal> <intednodePartitionFile>");
			System.exit(1);
		}

		try {
			new PartitionNodesToRealNodes(args[0], args[1]);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	public PartitionNodesToRealNodes(String nodeFileReal, String nodePartitionFile) throws FileNotFoundException, IOException {
		// load partition file into memory
		// stream nodePartition file
		// write realnode partition file

		HashMap<Integer, String> nodeidPartitionMap = new HashMap<Integer, String>();

		String line = "";

		BufferedReader partitionReader = FileUtil.getBufferedReader(nodePartitionFile);
		while ((line = partitionReader.readLine()) != null) {
			String parse[] = line.split("\t| ");
			nodeidPartitionMap.put(Integer.parseInt(parse[0]), parse[1]);
		}
		partitionReader.close();

		PrintWriter pw = FileUtil.getPrintWriter(nodePartitionFile + "-realuri.gz");
		line = "";
		BufferedReader realNodeReader = FileUtil.getBufferedReader(nodeFileReal);
		while ((line = realNodeReader.readLine()) != null) {
			String parse[] = line.split("\t| ");
			String partition = nodeidPartitionMap.get(Integer.parseInt(parse[1]));
			if (partition != null) {
				pw.write(parse[0] + " " + partition + "\n");
			}

		}
		realNodeReader.close();
		pw.close();
	}
}
