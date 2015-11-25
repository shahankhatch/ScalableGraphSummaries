package edu.toronto.cs.sgbhadoop.sortedpackage;

import edu.toronto.cs.sgbhadoop.util.FileUtil;
import edu.toronto.cs.sgbhadoop.util.Timer;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;

import org.apache.commons.codec.digest.DigestUtils;


/**
 * Compacts the integers of an already inted graph
 *
 */
public class ConvertToIntGraphchivsIntin {

	public static String EXT = ".gz";

	public static void main(String[] args) throws IOException {
		if (args.length != 4) {
			System.err.println("ConvertToIntGraphchivs <input> <output> <extension:.gz|.bz2>");
		}
		final String input = args[0];
		final String output = args[1];
		EXT = args[2];

		Timer t = new Timer("ConvertToInt", true);
		new ConvertToIntGraphchivsIntin(input, output);
		System.out.println(t.stop());
	}

	public ConvertToIntGraphchivsIntin(String input, String output) throws IOException {
		Timer t = new Timer("convert", true);

		BufferedReader bis = FileUtil.getBufferedReader(input);
		FileUtil.prepareOutputFolder(output);

		PrintWriter intedpw = FileUtil.getPrintWriter(output + File.separatorChar + "inted-clean" + EXT);
		PrintWriter nodeidspw = FileUtil.getPrintWriter(output + File.separatorChar + "nodeids" + EXT);

		PrintWriter predidspw = FileUtil.getPrintWriter(output + File.separatorChar + "predids" + EXT);

		int linenum = 0;

		String line = null;
		int id = 0;
		int predid = 0;

		mapped = new HashMap<Integer, Integer>(20000000);
		mappedPredicates = new HashMap<Integer, Integer>(2000);

		while ((line = bis.readLine()) != null) {
			if (linenum++ % 1000000 == 0) {
				System.out.println(t.toString());
				System.out.println("linenum-interm:" + (linenum - 1));
			}

			final String[] parse = line.split("\t| ");

			if (parse.length < 4) {
				// require a triple
				continue;
			}

			// register id for string if line passes muster
			for (int i = 0; i < 3; i++) {
				if (i == 1 && !mappedPredicates.containsKey(Integer.parseInt(parse[i]))) {
					mappedPredicates.put(Integer.parseInt(parse[i]), predid);
					predidspw.write(parse[i] + " " + predid + "\n");
					predid++;
				} else if (i != 1 && !mapped.containsKey(Integer.parseInt(parse[i]))) {
					mapped.put(Integer.parseInt(parse[i]), id);
					//					System.out.println(new Integer(id).byteValue());
					nodeidspw.write(parse[i] + " " + id + "\n");
					id++;
				}
			}

			writeLine(intedpw, parse);

		}

		bis.close();
		nodeidspw.close();
		intedpw.close();
		System.out.println("linenum:" + linenum);
		System.out.println("mapped:" + mapped.size());
		System.out.println("mappedPredicates:" + mappedPredicates.size());

	}

	private void writeLine(PrintWriter pw, final String[] parse) {
		final String newline = mapped.get(Integer.parseInt(parse[0])) + " " + mappedPredicates.get(Integer.parseInt(parse[1])) + " " + mapped.get(Integer.parseInt(parse[2])) + "\n";
		pw.write(newline);
	}

	private final HashMap<Integer, Integer> mapped;
	private final HashMap<Integer, Integer> mappedPredicates;

	public static String getMD5(String inString) {
		return DigestUtils.md5Hex(inString);
	}
}
