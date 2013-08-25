package org.clueweb.dictionary;

import java.io.PrintStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.clueweb.data.TermStatistics;

public class DumpDictionary {
  private static final Logger LOG = Logger.getLogger(DumpDictionary.class);

  public static void main(String[] args) throws Exception {
    if (args.length != 1) {
      System.err.println("usage: [index-path]");
      System.exit(-1);
    }

    String path = args[0];

    PrintStream out = new PrintStream(System.out, true, "UTF-8");

    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(conf);

    DefaultFrequencySortedDictionary dictionary = new DefaultFrequencySortedDictionary(path, fs);
    TermStatistics stats = new TermStatistics(new Path(path), fs);

    int nTerms = dictionary.size();
    String s = null;
    for (int i = 1; i <= nTerms; i++) {
      try {
        s = dictionary.getTerm(i);
      } catch (Exception e) {
        // Log and move on...
        s = "";
        LOG.error("Error fetching term for termid " + i);
      }

      out.println(i + "\t" + s + "\t" + stats.getDf(i) + "\t" + stats.getCf(i));
    }
    out.close();
  }
}
