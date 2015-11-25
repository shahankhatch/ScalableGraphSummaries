package edu.toronto.cs.sgbhadoop.sortedpackage;

import edu.toronto.cs.sgbhadoop.util.FileUtil;
import edu.toronto.cs.sgbhadoop.util.Timer;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ConvertToIntTwitter {

  private static final Logger LOG = Logger.getLogger(ConvertToIntTwitter.class.getName());
  
  private static final int TWITTER_INITIALMAPSIZE = 55_000_000;
  private final HashMap<Integer, Integer> mapped;

  private static String EXT = ".gz";

  // configured for links-anon.txt.gz
  public static void main(String[] args) throws IOException {
    if (args.length != 3) {
      LOG.severe("ConvertToIntGraphchivs <input> <output> <extension:.gz|.bz2>");
    }
    final String input = args[0];
    final String output = args[1];
    EXT = args[2];

    Timer t = new Timer("ConvertToInt", true);
    new ConvertToIntTwitter(input, output);
    LOG.log(Level.INFO, "{0}", t.stop());
  }

  public ConvertToIntTwitter(final String input, final String output) throws IOException {
    final Timer t = new Timer("convert", true);

    int linenum;
    try (final BufferedReader bis = FileUtil.getBufferedReader(input);
            final PrintWriter intedpw = FileUtil.getPrintWriter(output + File.separatorChar + "inted-clean" + EXT);
            final PrintWriter nodeidspw = FileUtil.getPrintWriter(output + File.separatorChar + "nodeids" + EXT);) {
      FileUtil.prepareOutputFolder(output);
      linenum = 0;
      String line;
      int id = 0;
      mapped = new HashMap<>(TWITTER_INITIALMAPSIZE);
      while ((line = bis.readLine()) != null) {
        if (linenum++ % 1_000_000 == 0) {
          LOG.info(t.toString());
          LOG.log(Level.INFO, "linenum:{0}", (linenum - 1));
        }

        final String[] parse = line.split("\t| ");

        // register id for string if line passes muster
        for (int i = 0; i < 2; i++) {
          if (!mapped.containsKey(Integer.parseInt(parse[i]))) {
            mapped.put(Integer.parseInt(parse[i]), id);
            //					System.out.println(new Integer(id).byteValue());
            nodeidspw.write(parse[i] + " " + id + "\n");
            nodeidspw.flush();
            id++;
          }
        }

        writeLine(intedpw, parse);
        intedpw.flush();

      }
    }
    LOG.log(Level.INFO, "linenum:{0}", linenum);
    LOG.log(Level.INFO, "mapped:{0}", mapped.size());

  }
  
  private void writeLine(PrintWriter pw, final String[] parse) {
    // configured so that input line is follower-followed, and output as followed-follower
    final String newline = mapped.get(Integer.parseInt(parse[1])) + " " + 0 + " " + mapped.get(Integer.parseInt(parse[0])) + "\n";
    pw.write(newline);
  }

}
