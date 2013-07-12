package org.clueweb.clueweb12.app;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.clueweb.data.WarcTrecIdMapping;

public class LookupWarcTrecIdMapping extends Configured implements Tool {
  private static final String INDEX_OPTION = "index";
  private static final String DOCID_OPTION = "docid";
  private static final String DOCNO_OPTION = "docno";

  @SuppressWarnings("static-access")
  public int run(String[] args) throws Exception {
    Options options = new Options();
    options.addOption(OptionBuilder.withArgName("dir").hasArg()
        .withDescription("index location").create(INDEX_OPTION));
    options.addOption(OptionBuilder.withArgName("id").hasArg()
        .withDescription("WARC-TREC-ID").create(DOCID_OPTION));
    options.addOption(OptionBuilder.withArgName("num").hasArg()
        .withDescription("docno").create(DOCNO_OPTION));

    CommandLine cmdline = null;
    CommandLineParser parser = new GnuParser();
    try {
      cmdline = parser.parse(options, args);
    } catch (ParseException exp) {
      System.err.println("Error parsing command line: " + exp.getMessage());
      System.exit(-1);
    }

    if (!cmdline.hasOption(INDEX_OPTION) || 
        !(cmdline.hasOption(DOCID_OPTION) || cmdline.hasOption(DOCNO_OPTION))) {
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp(LookupWarcTrecIdMapping.class.getCanonicalName(), options);
      System.exit(-1);
    }

    String indexPath = cmdline.getOptionValue(INDEX_OPTION);

    WarcTrecIdMapping mapping = new WarcTrecIdMapping(new Path(indexPath), getConf());
    if (cmdline.hasOption(DOCID_OPTION)) {
      System.out.println(mapping.getDocno(cmdline.getOptionValue(DOCID_OPTION)));
    }

    if (cmdline.hasOption(DOCNO_OPTION)) {
      System.out.println(mapping.getDocid(Integer.parseInt(cmdline.getOptionValue(DOCNO_OPTION))));
    }

    return 0;
  }

  public static void main(String[] args) throws Exception {
    ToolRunner.run(new LookupWarcTrecIdMapping(), args);
  }
}
