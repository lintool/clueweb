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
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.clueweb.data.DocVector;
import org.clueweb.data.DocVectorCompressor;
import org.clueweb.data.DocVectorCompressorFactory;
import org.clueweb.dictionary.DefaultFrequencySortedDictionary;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;

public class ProcessDocVectors extends Configured implements Tool {
  private static final Logger LOG = Logger.getLogger(ProcessDocVectors.class);

  private static class MyMapper extends Mapper<Text, Writable, Text, Text> {
    private static final Joiner JOINER = Joiner.on("|");

    private DefaultFrequencySortedDictionary dictionary;
    private DocVectorCompressor compressor;
    private DocVector doc;

    @Override
    public void setup(Context context) throws IOException {
      FileSystem fs = FileSystem.get(context.getConfiguration());
      String path = context.getConfiguration().get(DICTIONARY_OPTION);
      dictionary = new DefaultFrequencySortedDictionary(path, fs);

      String compressorType = context.getConfiguration().get(DOCVECTOR);
      compressor = DocVectorCompressorFactory.getCompressor(compressorType);
      if (compressor == null) {
        throw new RuntimeException("Error: docvector type '" + compressorType + "' not recognized!");
      }
      doc = compressor.createDocVector();
    }

    @Override
    public void map(Text key, Writable writable, Context context)
        throws IOException, InterruptedException {
      compressor.decompress(writable, doc);

      List<String> terms = Lists.newArrayList();
      for (int termid : doc.getTermIds()) {
        terms.add(dictionary.getTerm(termid));
      }

      context.write(key, new Text(JOINER.join(terms)));
    }
  }

  public static final String INPUT_OPTION = "input";
  public static final String OUTPUT_OPTION = "output";
  public static final String DICTIONARY_OPTION = "dictionary";
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
    String docvector = cmdline.hasOption(DOCVECTOR) ? cmdline.getOptionValue(DOCVECTOR) : "pfor";

    LOG.info("Tool name: " + ProcessDocVectors.class.getSimpleName());
    LOG.info(" - input: " + input);
    LOG.info(" - output: " + output);
    LOG.info(" - dictionary: " + dictionary);
    LOG.info(" - docvector: " + docvector);

    Job job = new Job(getConf(), ProcessDocVectors.class.getSimpleName() + ":" + input);
    job.setJarByClass(ProcessDocVectors.class);

    job.setNumReduceTasks(0);

    FileInputFormat.setInputPaths(job, input);
    FileOutputFormat.setOutputPath(job, new Path(output));

    job.getConfiguration().set(DICTIONARY_OPTION, dictionary);
    job.getConfiguration().set(DOCVECTOR, docvector);

    job.setInputFormatClass(SequenceFileInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);

    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);

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
    LOG.info("Running " + ProcessDocVectors.class.getCanonicalName() + " with args "
        + Arrays.toString(args));
    ToolRunner.run(new ProcessDocVectors(), args);
  }
}
