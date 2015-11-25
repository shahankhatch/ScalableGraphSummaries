package edu.toronto.cs.sgbhadoop.partition;

import edu.toronto.cs.sgbhadoop.util.FileUtil;
import edu.toronto.cs.sgbhadoop.util.Timer;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;


public class AugmentEdgeWRank {

	public static void main(String[] args) throws FileNotFoundException, IOException {
		if (args.length != 4) {
			System.err.println("args: <rankfile-fw> <rankfile-bw> <edgefile> <numvertices>");
			System.exit(1);
		}
		String rankfilefw = args[0];
		String rankfilebw = args[1];
		String edgefile = args[2];
		int numverts = Integer.parseInt(args[3]);

		int[] fwrank = new int[numverts];
		int[] bwrank = new int[numverts];

		Timer t1 = new Timer("fwfile", true);
		int linenum = 0;
		System.out.println("Reading fw file");
		BufferedReader fwrankfile = FileUtil.getBufferedReader(rankfilefw);
		String line = null;
		while ((line = fwrankfile.readLine()) != null) {
			if (linenum++ % 1000000 == 0) {
				System.out.println("linenum:" + linenum);
			}
			String parse[] = line.split("\t| ");
			fwrank[Integer.parseInt(parse[0])] = Integer.parseInt(parse[1]);
		}
		fwrankfile.close();
		System.out.println(t1.stop());

		Timer t2 = new Timer("bwfile", true);
		linenum = 0;
		System.out.println("Reading bw file");
		BufferedReader bwrankfile = FileUtil.getBufferedReader(rankfilebw);
		line = null;
		while ((line = bwrankfile.readLine()) != null) {
			if (linenum++ % 1000000 == 0) {
				System.out.println("linenum:" + linenum);
			}
			String parse[] = line.split("\t| ");
			bwrank[Integer.parseInt(parse[0])] = Integer.parseInt(parse[1]);
		}
		bwrankfile.close();
		System.out.println(t2.stop());

		Timer t = new Timer("edges", true);
		linenum = 0;
		System.out.println("Reading and writing edge file");
		PrintWriter outFile = FileUtil.getPrintWriter(edgefile + "-augmented.gz");
		BufferedReader edgeReader = FileUtil.getBufferedReader(edgefile);
		line = null;
		while ((line = edgeReader.readLine()) != null) {
			if (linenum++ % 1000000 == 0) {
				System.out.println("linenum:" + linenum);
			}
			String parse[] = line.split("\t| ");
			final int s = Integer.parseInt(parse[0]);
			final String p = parse[1];
			final int o = Integer.parseInt(parse[2]);
			final String outline = s + " " + p + " " + o + " " + fwrank[s] + " " + fwrank[o] + " " + bwrank[s] + " " + bwrank[o] + "\n";
			outFile.write(outline);
		}
		outFile.close();
		edgeReader.close();
		System.out.println(t.stop());

	}
}
