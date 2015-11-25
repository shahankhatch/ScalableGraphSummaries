package edu.toronto.cs.sgbhadoop.sortedpackage;

import edu.toronto.cs.sgbhadoop.util.FileUtil;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.PrintWriter;


/**
 * Generate summary extents. Saves statements of real uri partition extents to inputNodesFile+"-realpartitionstmts.gz"
 *
 */
public class ConvertPartitionToSummary {

	public static void main(String[] args) throws IOException {
		if (args.length != 1) {
			System.err.println("ConvertPartitionToSummary <input path to nodes>");
			System.exit(1);
		}

		final String inputPath = args[0];
		System.out.println("options:");
		System.out.println("input:" + inputPath);
		new ConvertPartitionToSummary(inputPath);
	}

	public ConvertPartitionToSummary(String inputNodesFile) throws IOException {
		BufferedReader nodesreader = FileUtil.getBufferedReader(inputNodesFile);

		PrintWriter summarywriter = FileUtil.getPrintWriter(inputNodesFile + "-realpartitionstmts.gz");

		String nodeline = null;
		while ((nodeline = nodesreader.readLine()) != null) {
			String[] parse = nodeline.split("\t| ");
			if (parse.length != 2 && parse.length != 3)
				continue;
			final String block = parse[1];
			String instance = parse[0];
			if (instance.endsWith(".")) {
				instance = instance.substring(0, instance.length() - 1);
			}
			if (!instance.startsWith("<")) {
				instance = "<" + instance + ">";
			}
			// for each node, output a statement of the form instance bc:inExtent block .
			final String newline = "<http://bc.org/" + block + "> <http://bc.org/hasExtent> " + instance + " .\n";
			summarywriter.write(newline);
		}

		summarywriter.close();
		nodesreader.close();
	}

}
