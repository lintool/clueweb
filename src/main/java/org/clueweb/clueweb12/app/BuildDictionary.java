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

import it.unimi.dsi.sux4j.mph.TwoStepsLcpMonotoneMinimalPerfectHashFunction;
import it.unimi.dsi.util.FrontCodedStringList;
import it.unimi.dsi.util.ShiftAddXorSignedStringMap;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.clueweb.dictionary.DictionaryTransformationStrategy;
import org.clueweb.util.QuickSort;

import tl.lin.data.pair.PairOfIntLong;

import com.google.common.collect.Lists;

public class BuildDictionary extends Configured implements Tool {
  private static final Logger LOG = Logger.getLogger(BuildDictionary.class);

  private static final String HADOOP_OUTPUT_OPTION = "dictionary.path";
  private static final String HADOOP_TERMS_COUNT_OPTION = "terms.count";

  protected static enum Terms { Total }

  public static final String TERMS_DATA = "dictionary.terms";
  public static final String TERMS_ID_DATA = "dictionary.ids";
  public static final String TERMS_ID_MAPPING_DATA = "dictionary.mapping";

  public static final String DF_BY_TERM_DATA = "df.terms";
  public static final String DF_BY_ID_DATA = "df.ids";

  public static final String CF_BY_TERM_DATA = "cf.terms";
  public static final String CF_BY_ID_DATA = "cf.ids";

  private static class MyReducer
      extends Reducer<Text, PairOfIntLong, NullWritable, NullWritable> {
    private FSDataOutputStream termsOut, idsOut, idsToTermOut,
        dfByTermOut, cfByTermOut, dfByIntOut, cfByIntOut;
    private int numTerms;
    private int[] seqNums = null;
    private int[] dfs = null;
    private long[] cfs = null;
    private int curKeyIndex = 0;

    private String[] terms;

    @Override
    public void setup(Reducer<Text, PairOfIntLong, NullWritable, NullWritable>.Context context)
        throws IOException {
      LOG.info("Starting setup.");
      Configuration conf = context.getConfiguration();
      FileSystem fs = FileSystem.get(conf);

      numTerms = conf.getInt(HADOOP_TERMS_COUNT_OPTION, 0);
      LOG.info(HADOOP_TERMS_COUNT_OPTION + ": " + numTerms);
      String basePath = conf.get(HADOOP_OUTPUT_OPTION);
      LOG.info(HADOOP_OUTPUT_OPTION + ": " + basePath);

      terms = new String[numTerms];
      seqNums = new int[numTerms];
      dfs = new int[numTerms];
      cfs = new long[numTerms];

      termsOut = fs.create(new Path(basePath, TERMS_DATA), true);

      idsOut = fs.create(new Path(basePath, TERMS_ID_DATA), true);
      idsOut.writeInt(numTerms);

      idsToTermOut = fs.create(new Path(basePath, TERMS_ID_MAPPING_DATA), true);
      idsToTermOut.writeInt(numTerms);

      dfByTermOut = fs.create(new Path(basePath, DF_BY_TERM_DATA), true);
      dfByTermOut.writeInt(numTerms);

      cfByTermOut = fs.create(new Path(basePath, CF_BY_TERM_DATA), true);
      cfByTermOut.writeInt(numTerms);

      dfByIntOut = fs.create(new Path(basePath, DF_BY_ID_DATA), true);
      dfByIntOut.writeInt(numTerms);

      cfByIntOut = fs.create(new Path(basePath, CF_BY_ID_DATA), true);
      cfByIntOut.writeInt(numTerms);
      LOG.info("Finished setup.");
    }

    @Override
    public void reduce(Text key, Iterable<PairOfIntLong> values, Context context)
        throws IOException, InterruptedException {
      String term = key.toString();
      Iterator<PairOfIntLong> iter = values.iterator();
      PairOfIntLong p = iter.next();
      int df = p.getLeftElement();
      long cf = p.getRightElement();
      WritableUtils.writeVInt(dfByTermOut, df);
      WritableUtils.writeVLong(cfByTermOut, cf);

      if (iter.hasNext()) {
        throw new RuntimeException("More than one record for term: " + term);
      }

      terms[curKeyIndex] = term;
      seqNums[curKeyIndex] = curKeyIndex;
      dfs[curKeyIndex] = -df;
      cfs[curKeyIndex] = cf;
      curKeyIndex++;

      context.getCounter(Terms.Total).increment(1);
    }

    @Override
    public void cleanup(
        Reducer<Text, PairOfIntLong, NullWritable, NullWritable>.Context context)
        throws IOException {
      LOG.info("Starting cleanup.");
      if (curKeyIndex != numTerms) {
        throw new RuntimeException("Total expected Terms: " + numTerms +
            ", Total observed terms: " + curKeyIndex + "!");
      }
      // Sort based on df and change seqNums accordingly.
      QuickSort.quicksortWithSecondary(seqNums, dfs, cfs, 0, numTerms - 1);

      // Write sorted dfs and cfs by int here.
      for (int i = 0; i < numTerms; i++) {
        WritableUtils.writeVInt(dfByIntOut, -dfs[i]);
        WritableUtils.writeVLong(cfByIntOut, cfs[i]);
      }
      cfs = null;

      // Encode the sorted dfs into ids ==> df values erased and become ids instead. Note that first
      // term id is 1.
      for (int i = 0; i < numTerms; i++) {
        dfs[i] = i + 1;
      }

      // Write current seq nums to be index into the term array.
      for (int i = 0; i < numTerms; i++)
        idsToTermOut.writeInt(seqNums[i]);

      // Sort on seqNums to get the right writing order.
      QuickSort.quicksort(dfs, seqNums, 0, numTerms - 1);
      for (int i = 0; i < numTerms; i++) {
        idsOut.writeInt(dfs[i]);
      }

      ByteArrayOutputStream bytesOut;
      ObjectOutputStream objOut;
      byte[] bytes;

      List<String> termList = Lists.newArrayList(terms);
      FrontCodedStringList frontcodedList = new FrontCodedStringList(termList, 8, true);

      bytesOut = new ByteArrayOutputStream();
      objOut = new ObjectOutputStream(bytesOut);
      objOut.writeObject(frontcodedList);
      objOut.close();

      bytes = bytesOut.toByteArray();
      termsOut.writeInt(bytes.length);
      termsOut.write(bytes);

      ShiftAddXorSignedStringMap dict = new ShiftAddXorSignedStringMap(termList.iterator(),
          new TwoStepsLcpMonotoneMinimalPerfectHashFunction<CharSequence>(termList,
              DictionaryTransformationStrategy.getStrategy()));

      bytesOut = new ByteArrayOutputStream();
      objOut = new ObjectOutputStream(bytesOut);
      objOut.writeObject(dict);
      objOut.close();

      bytes = bytesOut.toByteArray();
      termsOut.writeInt(bytes.length);
      termsOut.write(bytes);

      termsOut.close();
      idsOut.close();
      idsToTermOut.close();
      dfByTermOut.close();
      cfByTermOut.close();
      dfByIntOut.close();
      cfByIntOut.close();
      LOG.info("Finished cleanup.");
    }
  }

  public static final String INPUT_OPTION = "input";
  public static final String OUTPUT_OPTION = "output";
  public static final String COUNT_OPTION = "count";

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
        .withDescription("number of terms").create(COUNT_OPTION));

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
        !cmdline.hasOption(COUNT_OPTION)) {
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp(this.getClass().getName(), options);
      ToolRunner.printGenericCommandUsage(System.out);
      return -1;
    }

    String input = cmdline.getOptionValue(INPUT_OPTION);
    String output = cmdline.getOptionValue(OUTPUT_OPTION);

    LOG.info("Tool name: " + ComputeTermStatistics.class.getSimpleName());
    LOG.info(" - input: " + input);
    LOG.info(" - output: " + output);

    Configuration conf = getConf();

    conf.set(HADOOP_OUTPUT_OPTION, output);
    conf.setInt(HADOOP_TERMS_COUNT_OPTION,
        Integer.parseInt(cmdline.getOptionValue(COUNT_OPTION)));
    conf.set("mapreduce.map.memory.mb", "2048");
    conf.set("mapreduce.map.java.opts", "-Xmx2048m");
    conf.set("mapreduce.reduce.memory.mb", "2048");
    conf.set("mapreduce.reduce.java.opts", "-Xmx2048m");

    Job job = new Job(conf, BuildDictionary.class.getSimpleName() + ":" + input);

    job.setJarByClass(BuildDictionary.class);
    job.setNumReduceTasks(1);

    FileInputFormat.setInputPaths(job, new Path(input));
    FileOutputFormat.setOutputPath(job, new Path(output));

    job.setInputFormatClass(SequenceFileInputFormat.class);
    job.setOutputFormatClass(NullOutputFormat.class);

    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(PairOfIntLong.class);
    job.setOutputKeyClass(Text.class);
    job.setSortComparatorClass(DictionaryTransformationStrategy.WritableComparator.class);

    job.setMapperClass(Mapper.class);
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
    LOG.info("Running " + BuildDictionary.class.getCanonicalName() + " with args "
        + Arrays.toString(args));
    ToolRunner.run(new BuildDictionary(), args);
  }
}
