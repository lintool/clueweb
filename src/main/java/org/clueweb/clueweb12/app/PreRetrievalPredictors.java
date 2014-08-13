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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.clueweb.data.PForDocVector;
import org.clueweb.data.TermStatistics;
import org.clueweb.dictionary.DefaultFrequencySortedDictionary;
import org.clueweb.util.AnalyzerFactory;

import tl.lin.data.array.IntArrayWritable;
import tl.lin.data.pair.PairOfIntString;
import tl.lin.data.pair.PairOfStringFloat;
import tl.lin.lucene.AnalyzerUtils;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

/**
 * <p>
 * Implementation of a number of pre-retrieval predictors (i.e. those not based on the result list).
 * 
 * SumVAR, AvVAR, MaxVAR
 * - TF.IDF weight per query term/document w(t,d)
 * - average w across all documents
 * - standard deviation of those
 * map  -> runs over all documents, we know the query terms .. output [ (qid, t#1), count]
 *      -> we still require average                                   [ (qid, t#2), sum w(t,d)]
 *      ->                                                            [ (qid, t#3), w(t,d)]
 * 
 * @author Claudia Hauff
 */
public class PreRetrievalPredictors extends Configured implements Tool {

  private static final Logger LOG = Logger.getLogger(PreRetrievalPredictors.class);

  /*
   * Partitioner: all keys with the same qid go to the same reducer
   */
  private static class MyPartitioner extends Partitioner<PairOfIntString, FloatWritable> {

    @Override
    public int getPartition(PairOfIntString arg0, FloatWritable arg1, int numPartitions) {
      return arg0.getLeftElement() % numPartitions;
    }
  }

  /*
   * Mapper outKey: (qid,term#indicator), value: TF.IDF or count
   */
  protected static class MyMapper extends
      Mapper<Text, IntArrayWritable, PairOfIntString, FloatWritable> {

    private static final PForDocVector DOC = new PForDocVector();
    private DefaultFrequencySortedDictionary dictionary;
    protected TermStatistics stats;

    private static Analyzer ANALYZER;

    /*
     * set of all termids occurring in the queries
     */
    protected Set<Integer> termidSet;

    protected Map<Integer, Integer> counts;
    protected Map<Integer, Float> sums;
    
    // complex key: (qid,term#indicator)
    private static final PairOfIntString keyOut = new PairOfIntString();
    private static final FloatWritable valueOut = new FloatWritable();

    @Override
    public void setup(Context context) throws IOException {
      FileSystem fs = FileSystem.get(context.getConfiguration());
      String path = context.getConfiguration().get(DICTIONARY_OPTION);
      dictionary = new DefaultFrequencySortedDictionary(path, fs);
      stats = new TermStatistics(new Path(path), fs);

      String analyzerType = context.getConfiguration().get(PREPROCESSING);
      ANALYZER = AnalyzerFactory.getAnalyzer(analyzerType);
      if (ANALYZER == null) {
        LOG.error("Error: proprocessing type not recognized. Abort " + this.getClass().getName());
        return;
      }

      // read the queries from file
      termidSet = Sets.newHashSet();
      FSDataInputStream fsin = fs.open(new Path(context.getConfiguration().get(QUERIES_OPTION)));
      BufferedReader br = new BufferedReader(new InputStreamReader(fsin));
      String line;
      while ((line = br.readLine()) != null) {
        int index = line.indexOf(':');
        if (index < 0) {
          LOG.info("Query file line in incorrect format, expecting <num>:<term> <term>...\nInstead got:\n"
              + line);
          continue;
        }
        LOG.info("Parsing query line " + line);

        // normalize the terms (same way as the documents)
        for (String term : AnalyzerUtils.parse(ANALYZER, line.substring(index + 1))) {
          int termid = dictionary.getId(term);
          LOG.info("parsed term [" + term + "] has termid " + termid);

          if (termid < 0) {
            continue;
          }

          termidSet.add(termid);
        }
      }
      br.close();
      fsin.close();
    }

    @Override
    public void map(Text key, IntArrayWritable ints, Context context) throws IOException,
        InterruptedException {
      PForDocVector.fromIntArrayWritable(ints, DOC);
      
      // tfMap of the document
      HashMap<Integer, Integer> tfMap = Maps.newHashMap();
      for (int termid : DOC.getTermIds()) {
        int tf = 1;
        if (tfMap.containsKey(termid))
          tf += tfMap.get(termid);
        tfMap.put(termid, tf);
      }
      
      /*
      * map  -> runs over all documents, we know the query terms .. output [ (qid, t#1), count]
          *      -> we still require average                                   [ (qid, t#2), sum w(t,d)]
          *      ->                                                            [ (qid, t#3), w(t,d)]
      */
      for(int termid : termidSet) {
        if(tfMap.containsKey(termid)==false) {
          continue;
        }
        
        double df = stats.getDf(termid);
        float w = (float) (1 + Math.log(tfMap.get(termid)) * Math.log( 1 + stats.getCollectionSize() / stats.getDf(termid) ));
        
        if(counts.containsKey(termid)) {
          counts.put(termid,  counts.get(termid)+1);
        }
        else {
          counts.put(termid, 1);
        }
        
        if(sums.containsKey(termid)) {
          sums.put(termid, sums.get(termid)+w);
        }
        else {
          sums.put(termid, w);
        }
        
        keyOut.set(termid, "#3");
        valueOut.set(w);
        context.write(keyOut, valueOut);
      }
    }
    

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
      
      for(Integer termid : counts.keySet()) {
        keyOut.set(termid, "#1");
        valueOut.set(counts.get(termid));
        context.write(keyOut, valueOut);
        
        keyOut.set(termid, "#2");
        valueOut.set(sums.get(termid));
        context.write(keyOut, valueOut);
      }
    }
  }

  private static class MyReducer extends
      Reducer<PairOfIntString, FloatWritable, NullWritable, Text> {
 
    private static final NullWritable nullKey = NullWritable.get();
    private static final Text valueOut = new Text();

    private Map<Integer, Float> sums;
    private Map<Integer, Float> counts;
    private Map<Integer, Float> dev;
    
    @Override
    public void setup(Context context) {
      sums = Maps.newHashMap();
      counts = Maps.newHashMap();
      dev = Maps.newHashMap();
    }
    
    @Override
    public void reduce(PairOfIntString key, Iterable<FloatWritable> values, Context context)
        throws IOException, InterruptedException {
      
      int tid = key.getLeftElement();
      
      // #1 count
      // #2 sum
      // #3 are the individual weights
         
      double stdev = 0.0;
      
      for(FloatWritable f : values) {
        if(key.getRightElement().equals("#1")) {
          float c = f.get();
          if(counts.containsKey(tid)) {
            c += counts.get(tid);
          }
          counts.put(tid, c);
        }
        else if(key.getRightElement().equals("#2")) {
          float s = f.get();
          if(sums.containsKey(tid)) {
            s += sums.get(tid);
          }
          sums.put(tid, s);
        }
        else if(key.getRightElement().equals("#3")) { {
          float mean = sums.get(tid) / counts.get(tid);
          float w = (float)Math.pow(f.get()-mean, 2.0);
          if(dev.containsKey(tid)) {
            dev.put(tid, dev.get(tid)+w);
          }
        }
      }
    }

    // emit the scores for all queries
    public void cleanup(Context context) throws IOException, InterruptedException {
      
      Map<Integer, Double> maxMap = Maps.newHashMap();
      Map<Integer, Double> sumMap = Maps.newHashMap();
      Map<Integer, Double> countMap = Maps.newHashMap();
      
      for(String k : dev.keySet()) {
        int qid = Integer.parseInt(k.substring(0, k.indexOf('|')));
        
        double res = Math.sqrt(dev.get(k)/counts.get(k));
        if(maxMap.containsKey(qid)==false || res>maxMap.get(qid)) {
          maxMap.put(qid,  res);
        }
        if(sumMap.containsKey(qid)==false) {
          sumMap.put(qid, res);
        }
        else {
          sumMap.put( qid, sumMap.get(qid)+res );
        }
        if(countMap.containsKey(qid)==false) {
          countMap.put(qid,  1);
        }
        else {
          
        }
      }
      
    }
  }

  public static final String DOCVECTOR_OPTION = "docvector";
  public static final String OUTPUT_OPTION = "output";
  public static final String DICTIONARY_OPTION = "dictionary";
  public static final String QUERIES_OPTION = "queries";
  public static final String SMOOTHING = "smoothing";
  public static final String TOPK = "topk";
  public static final String PREPROCESSING = "preprocessing";

  /**
   * Runs this tool.
   */
  @SuppressWarnings({ "static-access" })
  public int run(String[] args) throws Exception {
    Options options = new Options();

    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("input path (pfor format expected, add * to retrieve files)")
        .create(DOCVECTOR_OPTION));
    options.addOption(OptionBuilder.withArgName("path").hasArg().withDescription("output path")
        .create(OUTPUT_OPTION));
    options.addOption(OptionBuilder.withArgName("path").hasArg().withDescription("dictionary")
        .create(DICTIONARY_OPTION));
    options.addOption(OptionBuilder.withArgName("path").hasArg().withDescription("queries")
        .create(QUERIES_OPTION));
    options.addOption(OptionBuilder.withArgName("float").hasArg().withDescription("smoothing")
        .create(SMOOTHING));
    options.addOption(OptionBuilder.withArgName("int").hasArg().withDescription("topk")
        .create(TOPK));
    options.addOption(OptionBuilder.withArgName("string " + AnalyzerFactory.getOptions()).hasArg()
        .withDescription("preprocessing").create(PREPROCESSING));

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
        || !cmdline.hasOption(DICTIONARY_OPTION) || !cmdline.hasOption(QUERIES_OPTION)
        || !cmdline.hasOption(SMOOTHING) || !cmdline.hasOption(TOPK)
        || !cmdline.hasOption(PREPROCESSING)) {
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp(this.getClass().getName(), options);
      ToolRunner.printGenericCommandUsage(System.out);
      return -1;
    }

    String docvector = cmdline.getOptionValue(DOCVECTOR_OPTION);
    String output = cmdline.getOptionValue(OUTPUT_OPTION);
    String dictionary = cmdline.getOptionValue(DICTIONARY_OPTION);
    String queries = cmdline.getOptionValue(QUERIES_OPTION);
    String smoothing = cmdline.getOptionValue(SMOOTHING);
    String topk = cmdline.getOptionValue(TOPK);
    String preprocessing = cmdline.getOptionValue(PREPROCESSING);

    LOG.info("Tool name: " + PreRetrievalPredictors.class.getSimpleName());
    LOG.info(" - docvector: " + docvector);
    LOG.info(" - output: " + output);
    LOG.info(" - dictionary: " + dictionary);
    LOG.info(" - queries: " + queries);
    LOG.info(" - smoothing: " + smoothing);
    LOG.info(" - topk: " + topk);
    LOG.info(" - preprocessing: " + preprocessing);

    Configuration conf = getConf();
    conf.set(DICTIONARY_OPTION, dictionary);
    conf.set(QUERIES_OPTION, queries);
    conf.setFloat(SMOOTHING, Float.parseFloat(smoothing));
    conf.setInt(TOPK, Integer.parseInt(topk));
    conf.set(PREPROCESSING, preprocessing);

    conf.set("mapreduce.map.memory.mb", "10048");
    conf.set("mapreduce.map.java.opts", "-Xmx10048m");
    conf.set("mapreduce.reduce.memory.mb", "10048");
    conf.set("mapreduce.reduce.java.opts", "-Xmx10048m");
    conf.set("mapred.task.timeout", "6000000"); // default is 600000

    FileSystem fs = FileSystem.get(conf);
    if (fs.exists(new Path(output))) {
      fs.delete(new Path(output), true);
    }

    Job job = new Job(conf, PreRetrievalPredictors.class.getSimpleName() + ":" + docvector);
    job.setJarByClass(PreRetrievalPredictors.class);

    FileInputFormat.setInputPaths(job, docvector);
    FileOutputFormat.setOutputPath(job, new Path(output));

    job.setInputFormatClass(SequenceFileInputFormat.class);

    job.setMapOutputKeyClass(PairOfIntString.class);
    job.setMapOutputValueClass(FloatWritable.class);
    job.setOutputKeyClass(NullWritable.class);
    job.setOutputValueClass(Text.class);

    job.setMapperClass(MyMapper.class);
    job.setPartitionerClass(MyPartitioner.class);
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
    LOG.info("Running " + PreRetrievalPredictors.class.getCanonicalName() + " with args "
        + Arrays.toString(args));
    ToolRunner.run(new PreRetrievalPredictors(), args);
  }
}
