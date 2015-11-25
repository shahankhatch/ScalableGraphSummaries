package edu.toronto.cs.sgbhadoop.partition;

import edu.toronto.cs.sgbhadoop.util.FileUtil;
import java.io.BufferedReader;
import java.io.PrintWriter;
import java.util.HashMap;


/**
 * Prints summary edges for given partition. Names output file intinststmts + ".summary-edges.gz"
 *
 */
public class PrintSummaryEdges {

	public static void main(String[] args) {
		if (args.length != 3) {
			System.out.println("args: <int-blockid-map-file> <predint-real-map-file> <int-instance-stmts>");
			System.exit(1);
		}
		String intblockid = args[0];
		String predintrealmap = args[1];
		String intinststmts = args[2];

		try {
			new PrintSummaryEdges(intblockid, predintrealmap, intinststmts);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public PrintSummaryEdges(String intblockid, String predintrealmap, String intinststmts) throws Exception {
		BufferedReader brpredid = FileUtil.getBufferedReader(predintrealmap);
		HashMap<Integer, String> predicatemap = new HashMap<Integer, String>();
		String strpredline = "";
		while ((strpredline = brpredid.readLine()) != null) {
			String parse[] = strpredline.split("\t| ");
			predicatemap.put(Integer.parseInt(parse[1]), parse[0]);
		}
		brpredid.close();

		HashMap<Integer, String> mapintblock = new HashMap<Integer, String>();

		BufferedReader brintblockid = FileUtil.getBufferedReader(intblockid);
		String strintblockid = "";
		while ((strintblockid = brintblockid.readLine()) != null) {
			final String parse[] = strintblockid.split("\t| ");
			final Integer nid = Integer.parseInt(parse[0]);
			final String bid = parse[1];
			mapintblock.put(nid, bid);
		}
		brintblockid.close();

		PrintWriter summedges = FileUtil.getPrintWriter(intblockid.replace(".gz", "") + ".summary-edges.nt.gz");
		BufferedReader brintinststmts = FileUtil.getBufferedReader(intinststmts);
		String strstmt = "";
		while ((strstmt = brintinststmts.readLine()) != null) {
			String parse[] = strstmt.split("\t| ");
			String newline = "<http://bc.org/" + mapintblock.get(Integer.parseInt(parse[0])) + "> " + predicatemap.get(Integer.parseInt(parse[1])) + " <http://bc.org/"
					+ mapintblock.get(Integer.parseInt(parse[2])) + "> .\n";
			summedges.write(newline);
		}
		summedges.flush();
		summedges.close();
		brintinststmts.close();

	}

}
