package edu.toronto.cs.sgbhadoop.partition;

import edu.toronto.cs.sgbhadoop.util.FileUtil;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;


public class PartitionRankedNodes {
	private static String outputfolder;

	public static void main(String[] args) {
		if (args.length != 2) {
			System.err.println("args: <inputnodesranked> <outputfolder>");
			System.exit(1);
		}
		String inedges = args[0];
		outputfolder = args[1];
		try {
			new PartitionRankedNodes(inedges);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static String getRankedNodesFilename(String sourcerank) {

		return outputfolder + File.separatorChar + "rankednodes_" + sourcerank + ".gz";
	}

	public PartitionRankedNodes(String inedges) throws FileNotFoundException, IOException {
		HashMap<String, PrintWriter> writers = new HashMap<String, PrintWriter>();

		BufferedReader inedgeReader = FileUtil.getBufferedReader(inedges);
		String line = null;
		int linenum = 0;
		// iterate and capture each edge into a writer
		while ((line = inedgeReader.readLine()) != null) {
			if (linenum++ % 1000000 == 0) {
				System.out.println("line:" + linenum);
			}
			String parse[] = line.split(" ");
			String sfw = parse[3];
			String fileString = getRankedNodesFilename(sfw);
			PrintWriter currentWriter = writers.get(fileString);
			if (currentWriter == null) {
				currentWriter = FileUtil.getPrintWriter(fileString);
				writers.put(fileString, currentWriter);
			}
			currentWriter.write(line + "\n");
		}
		// close all writers
		for (String key : writers.keySet()) {
			writers.get(key).close();
		}

	}
}
