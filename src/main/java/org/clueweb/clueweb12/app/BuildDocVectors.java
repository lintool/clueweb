/*
 * ClueWeb Tools: Hadoop tools for manipulating ClueWeb collections
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You may
 * obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package org.clueweb.clueweb12.app;

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
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.clueweb.clueweb12.ClueWeb12WarcRecord;
import org.clueweb.clueweb12.mapreduce.ClueWeb12InputFormat;
import org.clueweb.data.DocVectorCompressor;
import org.clueweb.data.DocVectorCompressorFactory;
import org.clueweb.dictionary.DefaultFrequencySortedDictionary;
import org.clueweb.util.AnalyzerFactory;
import org.jsoup.Jsoup;

import tl.lin.data.array.IntArrayWritable;
import tl.lin.lucene.AnalyzerUtils;

public class BuildDocVectors extends Configured implements Tool {
  private static final Logger LOG = Logger.getLogger(BuildDocVectors.class);

  private static enum Records {
    TOTAL, PAGES, ERRORS, TOO_LONG
  };

  private static class MyMapper extends
      Mapper<LongWritable, ClueWeb12WarcRecord, Text, Writable> {
    private static final int MAX_DOC_LENGTH = 512 * 1024; // Skip document if long than this.

    private static final Text DOCID = new Text();
    private static final IntArrayWritable DOC = new IntArrayWritable();

    private Analyzer analyzer;
    private DefaultFrequencySortedDictionary dictionary;
    private DocVectorCompressor compressor;

    @Override
    public void setup(Context context) throws IOException {
      FileSystem fs = FileSystem.get(context.getConfiguration());
      String path = context.getConfiguration().get(DICTIONARY_OPTION);
      dictionary = new DefaultFrequencySortedDictionary(path, fs);

      String analyzerType = context.getConfiguration().get(PREPROCESSING);
      analyzer = AnalyzerFactory.getAnalyzer(analyzerType);
      if (analyzer == null) {
        throw new RuntimeException("Error: proprocessing type '" + analyzerType + "' not recognized!");
      }

      String compressorType = context.getConfiguration().get(DOCVECTOR);
      compressor = DocVectorCompressorFactory.getCompressor(compressorType);
      if (compressor == null) {
        throw new RuntimeException("Error: dovector type '" + compressorType + "' not recognized!");
      }
    }

    @Override
    public void map(LongWritable key, ClueWeb12WarcRecord doc, Context context)
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
          // http://stackoverflow.com/questions/2275443/how-to-timeout-a-thread
          if (content.length() > MAX_DOC_LENGTH) {
            LOG.info("Skipping " + docid + " due to excessive length: " + content.length());
            context.getCounter(Records.TOO_LONG).increment(1);
            context.write(DOCID, compressor.emptyDocVector());
            return;
          }

          String cleaned = Jsoup.parse(content).text();
          List<String> tokens = AnalyzerUtils.parse(analyzer, cleaned);

          int len = 0;
          int[] termids = new int[tokens.size()];
          for (String token : tokens) {
            int id = dictionary.getId(token);
            if (id != -1) {
              termids[len] = id;
              len++;
            }
          }

          compressor.compress(DOC, termids, len);
          context.write(DOCID, DOC);
        } catch (Exception e) {
          // If Jsoup throws any exceptions, catch and move on, but emit empty doc.
          LOG.info("Error caught processing " + docid);
          context.getCounter(Records.ERRORS).increment(1);
          context.write(DOCID, compressor.emptyDocVector());
        }
      }
    }
  }

  public static final String INPUT_OPTION = "input";
  public static final String OUTPUT_OPTION = "output";
  public static final String DICTIONARY_OPTION = "dictionary";
  public static final String REDUCERS_OPTION = "reducers";
  public static final String PREPROCESSING = "preprocessing";
  public static final String DOCVECTOR = "docvector";

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
    options.addOption(OptionBuilder.withArgName("string " + AnalyzerFactory.getOptions()).hasArg()
        .withDescription("preprocessing").create(PREPROCESSING));
    options.addOption(OptionBuilder.withArgName("string " + DocVectorCompressorFactory.getOptions())
        .hasArg().withDescription("docvector").create(DOCVECTOR));

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

    if (!cmdline.hasOption(INPUT_OPTION) || !cmdline.hasOption(OUTPUT_OPTION)
        || !cmdline.hasOption(DICTIONARY_OPTION) || !cmdline.hasOption(PREPROCESSING)) {
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp(this.getClass().getName(), options);
      ToolRunner.printGenericCommandUsage(System.out);
      return -1;
    }

    String input = cmdline.getOptionValue(INPUT_OPTION);
    String output = cmdline.getOptionValue(OUTPUT_OPTION);
    String dictionary = cmdline.getOptionValue(DICTIONARY_OPTION);
    String preprocessing = cmdline.getOptionValue(PREPROCESSING);
    String docvector = cmdline.hasOption(DOCVECTOR) ? cmdline.getOptionValue(DOCVECTOR) : "pfor";

    Job job = new Job(getConf(), BuildDocVectors.class.getSimpleName() + ":" + input);
    job.setJarByClass(BuildDocVectors.class);

    LOG.info("Tool name: " + BuildDocVectors.class.getSimpleName());
    LOG.info(" - input: " + input);
    LOG.info(" - output: " + output);
    LOG.info(" - dictionary: " + dictionary);
    LOG.info(" - preprocessing: " + preprocessing);
    LOG.info(" - docvector: " + docvector);

    if (cmdline.hasOption(REDUCERS_OPTION)) {
      int numReducers = Integer.parseInt(cmdline.getOptionValue(REDUCERS_OPTION));
      LOG.info(" - reducers: " + numReducers);
      job.setNumReduceTasks(numReducers);
    } else {
      job.setNumReduceTasks(0);
    }

    DocVectorCompressor compressor = DocVectorCompressorFactory.getCompressor(docvector);
    FileInputFormat.setInputPaths(job, input);
    FileOutputFormat.setOutputPath(job, new Path(output));

    job.getConfiguration().set(DICTIONARY_OPTION, dictionary);
    job.getConfiguration().set(PREPROCESSING, preprocessing);
    job.getConfiguration().set(DOCVECTOR, docvector);

    job.setInputFormatClass(ClueWeb12InputFormat.class);
    job.setOutputFormatClass(SequenceFileOutputFormat.class);

    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(compressor.getCompressedClass());
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(compressor.getCompressedClass());

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
