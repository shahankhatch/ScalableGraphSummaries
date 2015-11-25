package edu.toronto.cs.sgbhadoop.partition;

import edu.toronto.cs.sgbhadoop.util.FileUtil;
import java.io.BufferedReader;
import java.io.PrintWriter;
import java.util.HashMap;


public class PrintMDLSummaryEdges {
	public PrintMDLSummaryEdges(String esizestatements, String blockextentsstatements, String summaryedgesfile) throws Exception {

		System.out.println("Reading/writing esizestatements");
		final HashMap<String, String> blockExtentMap = new HashMap<String, String>(50000000);
		final PrintWriter newesize = FileUtil.getPrintWriter(esizestatements + "-mdl.nt.gz");
		final BufferedReader esizebr = FileUtil.getBufferedReader(esizestatements);
		String line = "";
		int eonesizecount = 0;
		int esizecount = 0;
		while ((line = esizebr.readLine()) != null) {
			if (esizecount++ % 1000000 == 0) {
				System.out.println("esizecount:" + (esizecount - 1));
			}
			final String parse[] = line.split("\t| ");
			if (parse[2].contains("\"1\"")) {
				if (eonesizecount++ % 1000000 == 0) {
					System.out.println("eonesizecount:" + (eonesizecount - 1));
				}
				blockExtentMap.put(parse[0], null);
			} else {
				newesize.write(line + "\n");
			}
		}
		esizebr.close();
		newesize.flush();
		newesize.close();
		System.out.println("final eonesizecount:" + eonesizecount);

		System.out.println("Reading/writing blockextentsstatements");
		final PrintWriter newblockextents = FileUtil.getPrintWriter(blockextentsstatements + "-mdl.nt.gz");
		final BufferedReader blockextentsbr = FileUtil.getBufferedReader(blockextentsstatements);
		line = "";
		int mapped = 0;
		int blocklines = 0;
		while ((line = blockextentsbr.readLine()) != null) {
			if (blocklines++ % 1000000 == 0) {
				System.out.println("blocklines:" + (blocklines - 1));
			}
			final String parse[] = line.split("\t| ");
			if (blockExtentMap.containsKey(parse[0])) {
				blockExtentMap.put(parse[0], parse[2]);
				if (mapped++ % 1000000 == 0) {
					System.out.println("mapped:" + (mapped - 1));
				}
			} else {
				newblockextents.write(line + "\n");
			}
		}
		blockextentsbr.close();
		newblockextents.flush();
		newblockextents.close();
		System.out.println("final mapped:" + mapped);

		int linecount = 0;
		System.out.println("Reading/Writing summaryedgesfile");
		final PrintWriter pw = FileUtil.getPrintWriter(summaryedgesfile + "-mdl.nt.gz");
		final BufferedReader summaryedgesbr = FileUtil.getBufferedReader(summaryedgesfile);
		line = "";
		int replaced = 0;
		while ((line = summaryedgesbr.readLine()) != null) {
			if (linecount++ % 1000000 == 0) {
				System.out.println("line:" + (linecount - 1));
			}
			String parse[] = line.split("\t| ");
			final String sInst = blockExtentMap.get(parse[0]);
			final String oInst = blockExtentMap.get(parse[2]);
			if (sInst != null) {
				parse[0] = sInst;
				replaced++;
			}
			if (oInst != null) {
				parse[2] = oInst;
				replaced++;
			}
			pw.write(parse[0] + " " + parse[1] + " " + parse[2] + " .\n");
		}
		pw.flush();
		pw.close();
		summaryedgesbr.close();
		System.out.println("final replaced:" + replaced);
	}

	public static void main(String[] args) {
		if (args.length != 3) {
			System.err.println("args: <esizestatements> <blockextentsstatements> <summaryedgesfile>");
			System.exit(1);
		}
		final String esizestatements = args[0];
		final String blockextentsstatements = args[1];
		final String summaryedgesfile = args[2];
		try {
			new PrintMDLSummaryEdges(esizestatements, blockextentsstatements, summaryedgesfile);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
