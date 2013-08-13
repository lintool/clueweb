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
import java.util.Random;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.clueweb.data.PForDocVector;
import org.clueweb.data.WarcTrecIdMapping;

import tl.lin.data.array.IntArrayWritable;

public class DumpDocVectorsToRelations extends Configured implements Tool {
  private static final Logger LOG = Logger.getLogger(DumpDocVectorsToRelations.class);
  private static final Random RANDOM = new Random();

  private static enum Records { DOCS, TERMS };

  private static class MyReducer extends Reducer<Text, IntArrayWritable, NullWritable, NullWritable> {
    private static final PForDocVector DOC = new PForDocVector();

    private FSDataOutputStream docsOut;
    private FSDataOutputStream termsOut;
    private WarcTrecIdMapping mapping;

    @Override
    public void setup(Context context) throws IOException {
      FileSystem fs = FileSystem.get(context.getConfiguration());
      String basePath = context.getConfiguration().get(OUTPUT_OPTION);
      LOG.info(OUTPUT_OPTION + ": " + basePath);

      // Hard coded path for now: 
      mapping = new WarcTrecIdMapping(new Path("/data/private/clueweb12/derived/warc-trec-id-index-disk1/"),
          context.getConfiguration());

      int r = Math.abs(RANDOM.nextInt());
      // Yes, there's a small chance of collision, but number of partitions will
      // be relatively small.
      termsOut = fs.create(new Path(basePath, "terms-" + r + ".txt"), true);
      docsOut = fs.create(new Path(basePath, "docs-" + r + ".txt"), true);
    }

    @Override
    public void reduce(Text key, Iterable<IntArrayWritable> values, Context context)
        throws IOException, InterruptedException {
      IntArrayWritable ints = values.iterator().next();
      PForDocVector.fromIntArrayWritable(ints, DOC);

      int docno = mapping.getDocno(key.toString());
      if (docno < 0) {
        throw new RuntimeException("Unknown document: " + key.toString());
      }

      int pos = 0;
      for (int termid : DOC.getTermIds()) {
        context.getCounter(Records.TERMS).increment(1);
        termsOut.writeBytes(docno + "\t" + termid + "\t" + pos + "\n");
        pos++;
      }

      context.getCounter(Records.DOCS).increment(1);
      docsOut.writeBytes(docno + " " + key.toString() + "\n");
    }

    @Override
    public void cleanup(Context context)
        throws IOException {
      termsOut.close();
      docsOut.close();
    }
  }

  public static final String INPUT_OPTION = "input";
  public static final String OUTPUT_OPTION = "output";
  public static final String PARTITION_OPTION = "partitions";

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
    options.addOption(OptionBuilder.withArgName("num").hasArg()
        .withDescription("number of partitions").create(PARTITION_OPTION));

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
    int parts = cmdline.hasOption(PARTITION_OPTION) ?
        Integer.parseInt(cmdline.getOptionValue(PARTITION_OPTION)) : 10;

    LOG.info("Tool name: " + DumpDocVectorsToRelations.class.getSimpleName());
    LOG.info(" - input: " + input);
    LOG.info(" - output: " + output);
    LOG.info(" - partitions: " + parts);

    Job job = new Job(getConf(), DumpDocVectorsToRelations.class.getSimpleName() + ":" + input);
    job.setJarByClass(DumpDocVectorsToRelations.class);

    job.setNumReduceTasks(parts);

    FileInputFormat.setInputPaths(job, input);
    FileOutputFormat.setOutputPath(job, new Path(output));

    job.getConfiguration().set(OUTPUT_OPTION, output);
    job.getConfiguration().set("mapred.reduce.tasks.speculative.execution", "false");

    job.setInputFormatClass(SequenceFileInputFormat.class);
    job.setOutputFormatClass(NullOutputFormat.class);

    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IntArrayWritable.class);

    job.setReducerClass(MyReducer.class);

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
    LOG.info("Running " + DumpDocVectorsToRelations.class.getCanonicalName() + " with args "
        + Arrays.toString(args));
    ToolRunner.run(new DumpDocVectorsToRelations(), args);
  }
}
