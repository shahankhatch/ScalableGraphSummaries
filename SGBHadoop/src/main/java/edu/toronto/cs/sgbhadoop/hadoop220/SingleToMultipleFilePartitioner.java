/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.toronto.cs.sgbhadoop.hadoop220;

import edu.toronto.cs.sgbhadoop.util.FileUtil;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.PrintWriter;

/**
 *
 * @author SKX220
 */
public class SingleToMultipleFilePartitioner {

	public static void main(String[] args) throws IOException {
		if (args.length != 2) {
			System.out.println("args: <infile> <numparts>");
		}
		String infile = args[0];
		int numparts = Integer.parseInt(args[1]);

		new SingleToMultipleFilePartitioner(infile, numparts);
	}

	private SingleToMultipleFilePartitioner(String infile, int numparts) throws IOException {
		String baseOutFile = infile + "-partitioned-";

		PrintWriter[] pws = new PrintWriter[numparts];
		for (int i = 0; i < numparts; i++) {
			pws[i] = FileUtil.getPrintWriter(baseOutFile + i + ".gz");
		}

		try (BufferedReader bufferedReader = FileUtil.getBufferedReader(infile)) {
			String line = "";
			int linenum = 0;
			while ((line = bufferedReader.readLine()) != null) {
				pws[linenum++ % numparts].write(line+"\n");
			}
		}
		for (int i = 0; i<numparts;i++) {
			pws[i].flush();
			pws[i].close();
		}
	}

}
