package org.clueweb.clueweb12.app;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
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
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.util.Version;
import org.clueweb.clueweb12.data.ClueWarcRecord;
import org.clueweb.clueweb12.dictionary.DefaultFrequencySortedDictionary;
import org.clueweb.clueweb12.lucene.AnalyzerUtil;
import org.clueweb.clueweb12.mapreduce.ClueWarcInputFormat;
import org.jsoup.Jsoup;

public class BuildDocVectors extends Configured implements Tool {
  private static final Logger LOG = Logger.getLogger(BuildDocVectors.class);

  private static enum Records { TOTAL, PAGES, ERRORS, TOO_LONG };

  private static final Analyzer ANALYZER = new StandardAnalyzer(Version.LUCENE_43);

  private static final int MAX_DOC_LENGTH = 512 * 1024; // Skip document if long than this.

  private static class MyMapper extends Mapper<LongWritable, ClueWarcRecord, Text, BytesWritable> {
    private static final Text DOCID = new Text();
    private static final BytesWritable BYTES = new BytesWritable();
    private static final BytesWritable EMPTY = new BytesWritable();

    private DefaultFrequencySortedDictionary dictionary;

    @Override
    public void setup(Context context) throws IOException {
      FileSystem fs = FileSystem.get(context.getConfiguration());
      String path = context.getConfiguration().get(DICTIONARY_OPTION);
      dictionary = new DefaultFrequencySortedDictionary(path, fs);
    }

    @Override
    public void map(LongWritable key, ClueWarcRecord doc, Context context)
        throws IOException, InterruptedException {
      
      context.getCounter(Records.TOTAL).increment(1);

      String docid = doc.getHeaderMetadataItem("WARC-TREC-ID");
      if (docid != null) {
        DOCID.set(docid);

        context.getCounter(Records.PAGES).increment(1);
        try {
          String content = doc.getContent();

          // If the document is excessively long, it usually means that something is wrong (e.g., a
          // binary object). Skip so the parsing doesn't choke.
          // As an alternative, we might want to consider putting in a timeout, e.g.,
          //    http://stackoverflow.com/questions/2275443/how-to-timeout-a-thread
          if ( content.length() > MAX_DOC_LENGTH ) {
            LOG.info("Skipping " + docid + " due to excessive length: " + content.length());
            context.getCounter(Records.TOO_LONG).increment(1);
            context.write(DOCID, EMPTY);
            return;
          }

          String cleaned = Jsoup.parse(content).text();
          List<String> tokens = AnalyzerUtil.parse(ANALYZER, cleaned);

          ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
          DataOutputStream dataOut = new DataOutputStream(bytesOut);

          int len = 0;
          for (String token : tokens) {
            int id = dictionary.getId(token);
            if (id != -1) {
              WritableUtils.writeVInt(dataOut, id);
              len++;
            }
          }

          // Prepend the document length
          ByteArrayOutputStream finalOut = new ByteArrayOutputStream();
          DataOutputStream finalStream = new DataOutputStream(finalOut);
          WritableUtils.writeVInt(finalStream, len);
          finalStream.write(bytesOut.toByteArray());
          
          byte[] bytes = finalOut.toByteArray();
          BYTES.set(bytes, 0, bytes.length);
          context.write(DOCID, BYTES);
        }
        catch (Exception e) {
          // If Jsoup throws any exceptions, catch and move on, but emit empty doc.
          LOG.info("Error caught processing " + docid);
          context.getCounter(Records.ERRORS).increment(1);
          context.write(DOCID, EMPTY);
        }
      }
    }
  }

  public static final String INPUT_OPTION = "input";
  public static final String OUTPUT_OPTION = "output";
  public static final String DICTIONARY_OPTION = "dictionary";
  public static final String REDUCERS_OPTION = "reducers";

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
    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("dictionary").create(DICTIONARY_OPTION));
    options.addOption(OptionBuilder.withArgName("num").hasArg()
        .withDescription("number of reducers").create(REDUCERS_OPTION));

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

    if (!cmdline.hasOption(INPUT_OPTION) || !cmdline.hasOption(OUTPUT_OPTION) ||
        !cmdline.hasOption(DICTIONARY_OPTION)) {
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp(this.getClass().getName(), options);
      ToolRunner.printGenericCommandUsage(System.out);
      return -1;
    }

    String input = cmdline.getOptionValue(INPUT_OPTION);
    String output = cmdline.getOptionValue(OUTPUT_OPTION);
    String dictionary = cmdline.getOptionValue(DICTIONARY_OPTION);

    Job job = new Job(getConf(), BuildDocVectors.class.getSimpleName() + ":" + input);
    job.setJarByClass(BuildDocVectors.class);

    LOG.info("Tool name: " + BuildDocVectors.class.getSimpleName());
    LOG.info(" - input: " + input);
    LOG.info(" - output: " + output);
    LOG.info(" - dictionary: " + dictionary);

    if (cmdline.hasOption(REDUCERS_OPTION)) {
      int numReducers = Integer.parseInt(cmdline.getOptionValue(REDUCERS_OPTION));
      LOG.info(" - reducers: " + numReducers);
      job.setNumReduceTasks(numReducers);
    } else {
      job.setNumReduceTasks(0);
    }

    FileInputFormat.setInputPaths(job, input);
    FileOutputFormat.setOutputPath(job, new Path(output));

    job.getConfiguration().set(DICTIONARY_OPTION, dictionary);

    job.setInputFormatClass(ClueWarcInputFormat.class);
    job.setOutputFormatClass(SequenceFileOutputFormat.class);

    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(BytesWritable.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(BytesWritable.class);

    job.setMapperClass(MyMapper.class);

    FileSystem.get(getConf()).delete(new Path(output), true);

    long startTime = System.currentTimeMillis();
    job.waitForCompletion(true);
    LOG.info("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");

    return 0;
  }

  /**
   * Dispatches command-line arguments to the tool via the <code>ToolRunner</code>.
   */
  public static void main(String[] args) throws Exception {
    LOG.info("Running " + BuildDocVectors.class.getCanonicalName() + " with args "
        + Arrays.toString(args));
    ToolRunner.run(new BuildDocVectors(), args);
  }
}
