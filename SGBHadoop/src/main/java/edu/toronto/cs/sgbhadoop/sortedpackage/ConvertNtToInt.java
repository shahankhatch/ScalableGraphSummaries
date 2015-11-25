package edu.toronto.cs.sgbhadoop.sortedpackage;

import edu.toronto.cs.sgbhadoop.util.FileUtil;
import edu.toronto.cs.sgbhadoop.util.Timer;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;


/**
 * Latest convertor from nt to int without validation. Expects 3 columns and ignores remainder.
 *
 * @author SKX220
 *
 */
public class ConvertNtToInt {

  public static void main(String[] args) throws IOException {
    if (args.length != 2) {
      System.err.println("ConvertToInt <inputfile> <outputfolder> (remember to use -XmxYYG -XmsYYG JVM params)");
    }
    final String input = args[0];
    final String output = args[1];
    Timer t = new Timer("ConvertToInt", true);
    new ConvertNtToInt(input, output);
    System.out.println(t.stop());
  }

  public ConvertNtToInt(String input, String output) throws IOException {
    Timer t = new Timer("convert", true);

    try (
            BufferedReader bis = FileUtil.getBufferedReader(input);
            PrintWriter intedpw = FileUtil.getPrintWriter(output + File.separator + "inted-clean.gz");
            PrintWriter nodeidspw = FileUtil.getPrintWriter(output + File.separator + "nodeids.gz");
            PrintWriter predicateidspw = FileUtil.getPrintWriter(output + File.separator + "predicateids.gz");) {
      FileUtil.prepareOutputFolder(output);

      int linenum = 0;
      String line;
      final HashMap<String, Integer> mappedNodes = new HashMap<>();
      final HashMap<String, Integer> mappedPredicates = new HashMap<>();
      while ((line = bis.readLine()) != null) {
        if (linenum++ % 1000000 == 0) {
          System.out.println(t.toString());
          System.out.println("linenum:" + (linenum - 1));
        }

        final String[] parse = line.split("\t| ");
        if (parse.length < 3) {
          System.out.println("error: line length not satisfied");
          continue;
        }

        Integer sint = getId(mappedNodes, parse[0], nodeidspw);
        Integer pint = getId(mappedPredicates, parse[1], predicateidspw);
        Integer oint = getId(mappedNodes, parse[2], nodeidspw);

        writeLineReal(intedpw, new Integer[]{sint, pint, oint});
      }

      System.out.println("linenum:" + linenum);
      System.out.println("nodes:" + mappedNodes.size());
      System.out.println("predicates:" + mappedPredicates.size());
    }

  }

  private Integer getId(HashMap<String, Integer> mapped, String el, PrintWriter nodeidspw) {
    Integer j = mapped.get(el);
    if (j == null) {
      j = mapped.size();
      mapped.put(el, j);
      nodeidspw.write(el + " " + j + "\n");
    }
    return j;
  }

  private void writeLineReal(PrintWriter badiripw, final Integer[] parse) {
    final String newline = parse[0] + " " + parse[1] + " " + parse[2] + "\n";
    badiripw.write(newline);
  }

}
