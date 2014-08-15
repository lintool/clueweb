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
 * 
 * @author: Claudia Hauff
 * 
 * Implementation of the relevance language modeling approach (RM1 and RM3).
 * This code creates a model, but does not do a retrieval round with it.
 * 
 * 1. additional parameters: numFeedbackDocs, numFeedbackTerms
 * 
 * MyMapper:
 * - in setup() read the top numFeedbackDocs docids per query and compute the weight of each document
 * - in the map() walk over all documents; if a document is hit that is among the top results,
 *   compute P(w|D) x docWeight and emit (query,(term,weight))
 * 
 * MyReducer:
 * - in the reducer, add up all (term,weight) pairs and keep the top m; write those out to file query term P(w|R)
 */

package org.clueweb.clueweb12.app;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.TreeMap;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.clueweb.data.PForDocVector;
import org.clueweb.data.TermStatistics;
import org.clueweb.dictionary.DefaultFrequencySortedDictionary;
import org.clueweb.util.PairOfStringFloatComparator;
import org.clueweb.util.TRECResult;
import org.clueweb.util.TRECResultFileParser;

import tl.lin.data.array.IntArrayWritable;
import tl.lin.data.pair.PairOfFloatInt;
import tl.lin.data.pair.PairOfIntString;
import tl.lin.data.pair.PairOfStringFloat;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

public class RMModel extends Configured implements Tool {

  private static final Logger LOG = Logger.getLogger(RMModel.class);

  /*
   * Mapper outKey: (qid,docid), value: probability score
   */
  protected static class MyMapper extends
      Mapper<Text, IntArrayWritable, IntWritable, PairOfStringFloat> {

    private static final PForDocVector DOC = new PForDocVector();
    protected DefaultFrequencySortedDictionary dictionary;
    protected TermStatistics stats;

    private static final IntWritable keyOut = new IntWritable();
    protected PairOfStringFloat valueOut = new PairOfStringFloat();

    // query likelihood based document ranking
    protected static List<TRECResult> qlRanking;
    protected static HashSet<String> topRankedDocs;// a hashset for quick checking

    protected double smoothingParam;
    protected int numFeedbackDocs;

    @Override
    public void setup(Context context) throws IOException {

      FileSystem fs = FileSystem.get(context.getConfiguration());
      String path = context.getConfiguration().get(DICTIONARY_OPTION);
      dictionary = new DefaultFrequencySortedDictionary(path, fs);
      stats = new TermStatistics(new Path(path), fs);

      smoothingParam = context.getConfiguration().getFloat(SMOOTHING, 1000f);
      LOG.info("Smoothing set to " + smoothingParam);

      numFeedbackDocs = context.getConfiguration().getInt(NUM_FEEDBACK_DOCS, 20);

      LOG.info("Number of feedback documents set to: " + numFeedbackDocs);

      qlRanking = new ArrayList<TRECResult>();
      topRankedDocs = Sets.newHashSet();
      int currentQuery = -1;
      int numResults = 0;

      // read the TREC result file of the initial retrieval run as input;
      // keep the top N documents per query
      TRECResultFileParser parser = new TRECResultFileParser(fs, new Path(context
          .getConfiguration().get(TREC_RESULT_FILE)));
      while (parser.hasNext()) {
        TRECResult tr = parser.getNext();
        // a negative value is assumed to be the log of a prob
        tr.score = (tr.score < 0) ? Math.exp(tr.score) : tr.score;

        if (tr.qid != currentQuery) {
          numResults = 0;
        }

        currentQuery = tr.qid;

        if (numResults < numFeedbackDocs) {
          qlRanking.add(tr);
          topRankedDocs.add(tr.did);
          numResults++;
        }
      }

      // normalize the weights of the documents kept per query
      HashMap<Integer, Double> scoreSum = Maps.newHashMap();
      for (TRECResult tr : qlRanking) {
        double sum = tr.score;
        if (scoreSum.containsKey(tr.qid)) {
          sum += scoreSum.get(tr.qid);
        }
        scoreSum.put(tr.qid, sum);
      }

      for (TRECResult tr : qlRanking) {
        tr.score = tr.score / scoreSum.get(tr.qid);
      }
    }

    @Override
    public void map(Text key, IntArrayWritable ints, Context context) throws IOException,
        InterruptedException, IllegalStateException {

      PForDocVector.fromIntArrayWritable(ints, DOC);

      // are we interested in this document?
      if (topRankedDocs.contains(key.toString()) == false) {
        return;
      }

      //note: if set >1, the test cases using the toy corpus will not run successfully
      if (DOC.getLength() < 1) {
        return;
      }

      // tfMap of the document
      TreeMap<Integer, Integer> tfMap = Maps.newTreeMap();
      for (int termid : DOC.getTermIds()) {
        int tf = 1;
        if (tfMap.containsKey(termid)) {
          tf += tfMap.get(termid);
        }
        tfMap.put(termid, tf);
      }

      // compute P(term|doc) for each term occurring in the doc
      for (Integer termid : tfMap.keySet()) {

        String term = dictionary.getTerm(termid);
        if(term == null ) {
          continue;
        }
        double tf = tfMap.get(termid);
        double df = stats.getDf(termid);

        double mlProb = tf / (double) DOC.getLength();
        double colProb = df / (double) stats.getCollectionSize();

        double prob = 0.0;

        // JM smoothing
        if (smoothingParam <= 1.0) {
          prob = smoothingParam * mlProb + (1.0 - smoothingParam) * colProb;
        }
        // Dirichlet smoothing
        else {
          prob = (double) (tf + smoothingParam * colProb)
              / (double) (DOC.getLength() + smoothingParam);
        }

        for (TRECResult tr : qlRanking) {
          if (tr.did.equals(key.toString()) == false) {
            continue;
          }
          float termScore = (float) (prob * tr.score);
          keyOut.set(tr.qid);
          valueOut.set(term, termScore);
          context.write(keyOut, valueOut);
        }
      }
    }
  }

  private static class MyReducer extends
      Reducer<IntWritable, PairOfStringFloat, Text, FloatWritable> {

    private static final Text keyOut = new Text();
    private static final FloatWritable valueOut = new FloatWritable();

    private int numFeedbackTerms;

    @Override
    public void setup(Context context) throws IOException {
      numFeedbackTerms = context.getConfiguration().getInt(NUM_FEEDBACK_DOCS, 20);
    }

    @Override
    public void reduce(IntWritable key, Iterable<PairOfStringFloat> values, Context context)
        throws IOException, InterruptedException {

      HashMap<String, Double> termMap = Maps.newHashMap();

      for (PairOfStringFloat p : values) {
        String term = p.getLeftElement();
        float score = p.getRightElement();

        if (termMap.containsKey(term)) {
          score += termMap.get(term);
        }
        termMap.put(term, (double) score);
      }

      // priority queue to only keep the highest scoring terms
      PriorityQueue<PairOfStringFloat> queue = new PriorityQueue<PairOfStringFloat>(
          numFeedbackTerms, new PairOfStringFloatComparator());

      for (String term : termMap.keySet()) {
        if (queue.size() >= numFeedbackTerms && queue.peek().getRightElement() < termMap.get(term)) {
          queue.remove();
        }

        if (queue.size() < numFeedbackTerms) {
          queue.add(new PairOfStringFloat(term, termMap.get(term).floatValue()));
        }
      }

      // normalize the weights to sum to one
      double sum = 0.0;
      Iterator<PairOfStringFloat> it = queue.iterator();
      while (it.hasNext()) {
        sum += it.next().getRightElement();
      }

      // final step: output the normalized <term, weight> pairs
      it = queue.iterator();
      while (it.hasNext()) {
        PairOfStringFloat p = it.next();
        keyOut.set(key.toString() + "\t" + p.getLeftElement());
        double normalized = p.getRightElement() / sum;
        valueOut.set((float) normalized);
        context.write(keyOut, valueOut);
      }
    }
  }

  public static final String DOCVECTOR_OPTION = "docvector";
  public static final String OUTPUT_OPTION = "output";
  public static final String NUM_FEEDBACK_DOCS = "numFeedbackDocs";
  public static final String NUM_FEEDBACK_TERMS = "numFeedbackTerms";
  public static final String TREC_RESULT_FILE = "trecinputfile";
  public static final String DICTIONARY_OPTION = "dictionary";
  public static final String SMOOTHING = "smoothing";

  /**
   * Runs this tool.
   */
  @SuppressWarnings({ "static-access", "deprecation" })
  public int run(String[] args) throws Exception {
    Options options = new Options();

    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("input path (pfor format expected, add * to retrieve files)")
        .create(DOCVECTOR_OPTION));
    options.addOption(OptionBuilder.withArgName("path").hasArg().withDescription("output path")
        .create(OUTPUT_OPTION));
    options.addOption(OptionBuilder.withArgName("path").hasArg().withDescription("dictionary")
        .create(DICTIONARY_OPTION));
    options.addOption(OptionBuilder.withArgName("int").hasArg().withDescription("numFeedbackDocs")
        .create(NUM_FEEDBACK_DOCS));
    options.addOption(OptionBuilder.withArgName("int").hasArg().withDescription("numFeedbackTerms")
        .create(NUM_FEEDBACK_TERMS));
    options.addOption(OptionBuilder.withArgName("path").hasArg().withDescription("input path")
        .create(TREC_RESULT_FILE));
    options.addOption(OptionBuilder.withArgName("float").hasArg().withDescription("smoothing")
        .create(SMOOTHING));

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

    if (!cmdline.hasOption(DOCVECTOR_OPTION) || !cmdline.hasOption(OUTPUT_OPTION)
        || !cmdline.hasOption(DICTIONARY_OPTION) || !cmdline.hasOption(TREC_RESULT_FILE)
        || !cmdline.hasOption(NUM_FEEDBACK_DOCS) || !cmdline.hasOption(SMOOTHING)
        || !cmdline.hasOption(NUM_FEEDBACK_TERMS)) {
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp(this.getClass().getName(), options);
      ToolRunner.printGenericCommandUsage(System.out);
      return -1;
    }

    String docvector = cmdline.getOptionValue(DOCVECTOR_OPTION);
    String output = cmdline.getOptionValue(OUTPUT_OPTION);
    String dictionary = cmdline.getOptionValue(DICTIONARY_OPTION);
    String trecinput = cmdline.getOptionValue(TREC_RESULT_FILE);
    float smoothing = Float.parseFloat(cmdline.getOptionValue(SMOOTHING));
    int numDocs = Integer.parseInt(cmdline.getOptionValue(NUM_FEEDBACK_DOCS));
    int numTerms = Integer.parseInt(cmdline.getOptionValue(NUM_FEEDBACK_TERMS));

    LOG.info("Tool name: " + RMModel.class.getSimpleName());
    LOG.info(" - docvector: " + docvector);
    LOG.info(" - output: " + output);
    LOG.info(" - dictionary: " + dictionary);
    LOG.info(" - trecinputfile: " + trecinput);
    LOG.info(" - numFeedbackDocs: " + numDocs);
    LOG.info(" - numFeedbackTerms: " + numTerms);
    LOG.info(" - smoothing: " + smoothing);

    Configuration conf = getConf();
    conf.set(DICTIONARY_OPTION, dictionary);
    conf.set(TREC_RESULT_FILE, trecinput);
    conf.setInt(NUM_FEEDBACK_DOCS, numDocs);
    conf.setInt(NUM_FEEDBACK_TERMS, numTerms);
    conf.setFloat(SMOOTHING, smoothing);

    conf.set("mapreduce.map.memory.mb", "10048");
    conf.set("mapreduce.map.java.opts", "-Xmx10048m");
    conf.set("mapreduce.reduce.memory.mb", "10048");
    conf.set("mapreduce.reduce.java.opts", "-Xmx10048m");
    conf.set("mapred.task.timeout", "6000000");// default is 600000

    FileSystem fs = FileSystem.get(conf);
    if (fs.exists(new Path(output)))
      fs.delete(new Path(output));

    Job job = new Job(conf, RMModel.class.getSimpleName() + ":" + docvector);
    job.setJarByClass(RMModel.class);

    FileInputFormat.setInputPaths(job, docvector);
    FileOutputFormat.setOutputPath(job, new Path(output));

    job.setInputFormatClass(SequenceFileInputFormat.class);

    job.setMapOutputKeyClass(IntWritable.class);
    job.setMapOutputValueClass(PairOfStringFloat.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(FloatWritable.class);

    job.setMapperClass(MyMapper.class);
    job.setReducerClass(MyReducer.class);

    long startTime = System.currentTimeMillis();
    job.waitForCompletion(true);
    LOG.info("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");
    return 0;
  }

  /**
   * Dispatches command-line arguments to the tool via the <code>ToolRunner</code>.
   */
  public static void main(String[] args) throws Exception {
    LOG.info("Running " + RMModel.class.getCanonicalName() + " with args " + Arrays.toString(args));
    ToolRunner.run(new RMModel(), args);
  }
}
