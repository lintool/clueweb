package org.clueweb.clueweb12.app;

import java.io.IOException;
import java.io.StringReader;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.util.Version;
import org.clueweb.clueweb12.data.ClueWarcRecord;
import org.clueweb.clueweb12.mapred.ClueWarcInputFormat;
import org.jsoup.Jsoup;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;

public class DumpClueWarcRecordsToPlainText extends Configured implements Tool {
  private static final Logger LOG = Logger.getLogger(DumpClueWarcRecordsToPlainText.class);

  private static enum Records { TOTAL, PAGES, ERRORS };
  private static final Analyzer ANALYZER = new StandardAnalyzer(Version.LUCENE_43);
  private static final Joiner JOINER = Joiner.on("|");

  private static class MyMapper extends MapReduceBase implements
      Mapper<Writable, ClueWarcRecord, Text, Text> {
    private static final Text KEY = new Text();
    private static final Text VALUE = new Text();

    public void configure(JobConf job) {}

    public void map(Writable key, ClueWarcRecord doc, OutputCollector<Text, Text> output,
        Reporter reporter) throws IOException {
      reporter.incrCounter(Records.TOTAL, 1);

      String docid = doc.getHeaderMetadataItem("WARC-TREC-ID");
      if (docid != null) {
        reporter.incrCounter(Records.PAGES, 1);
        try {
          KEY.set(docid);
          String cleaned = Jsoup.parse(doc.getContent()).text().replaceAll("[\\r\\n]+", " ");
          cleaned = JOINER.join(parseKeywords(ANALYZER, cleaned));
          VALUE.set(cleaned);
          output.collect(KEY, VALUE);
        } catch (Exception e) {
          // If Jsoup throws any exceptions, catch and move on.
          reporter.incrCounter(Records.ERRORS, 1);
        }
      }
    }
  }

  static private List<String> parseKeywords(Analyzer analyzer, String keywords) throws IOException {
    List<String> list = Lists.newArrayList();

    TokenStream tokenStream = analyzer.tokenStream(null, new StringReader(keywords));
    CharTermAttribute cattr = tokenStream.addAttribute(CharTermAttribute.class);
    tokenStream.reset();
    while (tokenStream.incrementToken()) {
      if (cattr.toString().length() == 0) {
        continue;
      }
      list.add(cattr.toString());
    }
    tokenStream.end();
    tokenStream.close();

    return list;
  }

  public DumpClueWarcRecordsToPlainText() {
  }

  public static final String INPUT_OPTION = "input";
  public static final String OUTPUT_OPTION = "output";

  /**
   * Runs this tool.
   */
  @SuppressWarnings("static-access")
  public int run(String[] args) throws Exception {
    Options options = new Options();

    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("input path").create(INPUT_OPTION));
    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("output path").create(OUTPUT_OPTION));

    CommandLine cmdline;
    CommandLineParser parser = new GnuParser();
    try {
      cmdline = parser.parse(options, args);
    } catch (ParseException exp) {
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp(this.getClass().getName(), options);
      ToolRunner.printGenericCommandUsage(System.out);
      System.err.println("Error parsing command line: " + exp.getMessage());
      return -1;
    }

    if (!cmdline.hasOption(INPUT_OPTION) || !cmdline.hasOption(OUTPUT_OPTION)) {
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp(this.getClass().getName(), options);
      ToolRunner.printGenericCommandUsage(System.out);
      return -1;
    }

    String input = cmdline.getOptionValue(INPUT_OPTION);
    String output = cmdline.getOptionValue(OUTPUT_OPTION);

    LOG.info("Tool name: " + DumpClueWarcRecordsToPlainText.class.getSimpleName());
    LOG.info(" - input: " + input);
    LOG.info(" - output: " + output);

    JobConf conf = new JobConf(getConf(), DumpClueWarcRecordsToPlainText.class);
    conf.setJobName(DumpClueWarcRecordsToPlainText.class.getSimpleName() + ":" + input);

    conf.setNumReduceTasks(0);

    FileInputFormat.addInputPaths(conf, input);
    FileOutputFormat.setOutputPath(conf, new Path(output));

    conf.setInputFormat(ClueWarcInputFormat.class);
    conf.setOutputFormat(TextOutputFormat.class);
    conf.setMapperClass(MyMapper.class);

    RunningJob job = JobClient.runJob(conf);
    Counters counters = job.getCounters();
    int numDocs = (int) counters.findCounter(Records.PAGES).getCounter();

    LOG.info("Read " + numDocs + " docs.");

    return 0;
  }

  /**
   * Dispatches command-line arguments to the tool via the <code>ToolRunner</code>.
   */
  public static void main(String[] args) throws Exception {
    LOG.info("Running " + DumpClueWarcRecordsToPlainText.class.getCanonicalName() + " with args "
        + Arrays.toString(args));
    ToolRunner.run(new DumpClueWarcRecordsToPlainText(), args);
  }
}