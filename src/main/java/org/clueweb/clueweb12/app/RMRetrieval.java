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
 * Implementation of language modeling. Retrieval parameter <i>smoothing</i> determines the type: 
 * 		smoothing<=1 means Jelineck-Mercer and smoothing>1 means Dirichlet.
 * 
 * Approach:
 * 
 * (1) read the queries and convert into termids (based on the dictionary);
 * 			make sure to use the same Lucene Analyzer as in ComputeTermStatistics.java
 * 
 * (2) MyMapper: walk over all document vectors
 * 			2.1 determine all queries which have at least one query term is occurring in the document
 * 			2.2 for each such query, compute the LM score and emit composite key: (qid,docid), value: (score)
 * 
 * (3) MyPartitioner: ensure that all keys (qid,docid) with the same qid end up in the same reducer
 * 
 * (4) MyReducer: for each query
 * 			4.1 create a priority queue (minimum heap): we only need to keep the topk highest probability scores
 * 			4.2 once all key/values are processed, "sort" the doc/score elements in the priority queue (already semi-done in heap)
 * 			4.3 output the results in TREC result file format
 * 
 */

package org.clueweb.clueweb12.app;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
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
import org.clueweb.util.PairOfStringFloatComparator;

import tl.lin.data.array.IntArrayWritable;
import tl.lin.data.pair.PairOfIntString;
import tl.lin.data.pair.PairOfStringFloat;
import tl.lin.lucene.AnalyzerUtils;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

public class RMRetrieval extends Configured implements Tool {

  private static final Logger LOG = Logger.getLogger(RMRetrieval.class);

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
   * relevance language model;
   * addTerm() should be used to read the RMModel output file
   * interpolateWithQueryTerm() should be used to interpolate with the max. likelihood query model
   * 
   * RM1: do not call interpolateWithQueryTerm() or use queryLambda=0.0
   * RM3: queryLambda>0
   */
  private static class RelLM {
    public int qid;
    public HashMap<Integer, Double> probMap;

    public RelLM(int qid) {
      this.qid = qid;
      probMap = Maps.newHashMap();
    }

    public void addTerm(int termid, double weight) {
      probMap.put(termid, weight);
    }

    // RM3 is an interpolation between the query max. likelihood LM and the RM model
    public void interpolateWithQueryTerm(int termid, double weight, double queryLambda) {
      double prob = queryLambda * weight;
      if (probMap.containsKey(termid)) {
        prob += (1.0 - queryLambda) * probMap.get(termid);
      }
      probMap.put(termid, prob);
    }
    
    public boolean containsTerm(int termid) {
      return probMap.containsKey(termid);
    }
    
    public double getWeight(int termid) {
      if(!containsTerm(termid)) {
        return 0.0;
      }
      return probMap.get(termid);
    }
    
    
  }

  /*
   * Mapper outKey: (qid,docid), value: probability score
   */
  private static class MyMapper extends
      Mapper<Text, IntArrayWritable, PairOfIntString, FloatWritable> {

    private static final PForDocVector DOC = new PForDocVector();
    private DefaultFrequencySortedDictionary dictionary;
    private TermStatistics stats;
    private double smoothingParam;
    private double queryLambda;

    private static Analyzer ANALYZER;
    
    private static HashMap<Integer, RelLM> relLMMap;

    // complex key: (qid,docid)
    private static final PairOfIntString keyOut = new PairOfIntString();
    // value: float; probability score log(P(q|d))
    private static final FloatWritable valueOut = new FloatWritable();// score

    @Override
    public void setup(Context context) throws IOException {

      FileSystem fs = FileSystem.get(context.getConfiguration());
      String path = context.getConfiguration().get(DICTIONARY_OPTION);
      dictionary = new DefaultFrequencySortedDictionary(path, fs);
      stats = new TermStatistics(new Path(path), fs);

      smoothingParam = context.getConfiguration().getFloat(SMOOTHING, 1000f);
      LOG.info("Smoothing set to " + smoothingParam);
      
      queryLambda = context.getConfiguration().getFloat(QUERY_LAMBDA, 0.6f);
      
      /*
       * Read the relevance model from file
       */
      relLMMap = Maps.newHashMap();
      
      FSDataInputStream fsin = fs.open(new Path(context.getConfiguration().get(RMMODEL)));
      BufferedReader br = new BufferedReader(new InputStreamReader(fsin));
      String line;
      while ((line = br.readLine()) != null) {
        
        String tokens[] = line.split("\\s+");
        int qid = Integer.parseInt(tokens[0]);
        String term = tokens[1];
        double weight = Double.parseDouble(tokens[2]);
        
        RelLM rellm = null;
        if(relLMMap.containsKey(qid)) {
          rellm = relLMMap.get(qid);
        }
        else {
          rellm = new RelLM(qid);
          relLMMap.put(qid, rellm);
        }
        rellm.addTerm(dictionary.getId(term), weight);
      }

      br.close();
      fsin.close();
    
      
      /*
       * Interpolate the relevance model with the query maximum likelihood model
       */
      String analyzerType = context.getConfiguration().get(PREPROCESSING);
      ANALYZER = AnalyzerFactory.getAnalyzer(analyzerType);
      if (ANALYZER == null) {
        LOG.error("Error: proprocessing type not recognized. Abort " + this.getClass().getName());
        System.exit(1);
      }

      fsin = fs.open(new Path(context.getConfiguration().get(QUERIES_OPTION)));
      br = new BufferedReader(new InputStreamReader(fsin));
      while ((line = br.readLine()) != null) {
        int index = line.indexOf(':');
        if (index < 0) {
          LOG.info("Query file line in incorrect format, expecting <num>:<term> <term>...\n,instead got:\n"
              + line);
          continue;
        }
        int qid = Integer.parseInt(line.substring(0, index));

        HashSet<Integer> termSet = Sets.newHashSet();
        // normalize the terms (same way as the documents)
        for (String term : AnalyzerUtils.parse(ANALYZER, line.substring(index + 1))) {

          int termid = dictionary.getId(term);
          if(termid>0) {
            termSet.add(termid);
          }
        }
        
        if(termSet.size()==0) {
          continue;
        }
        
        RelLM rellm = null;
        if(relLMMap.containsKey(qid)) {
          rellm = relLMMap.get(qid);
        }
        else {
          rellm = new RelLM(qid);
          relLMMap.put(qid, rellm);
        }
        
        double termWeight = 1.0/(double)termSet.size();
        for(int termid : termSet) {
          rellm.interpolateWithQueryTerm(termid, termWeight, queryLambda);
        }
      }
      br.close();
      fsin.close();
      
      for(int qid : relLMMap.keySet())
      {
        LOG.info("++++ relevance LM qid="+qid+"++++++");
        HashMap<Integer, Double> probMap = relLMMap.get(qid).probMap;
        for(int termid : probMap.keySet()) {
          LOG.info("termid "+termid+" => "+probMap.get(termid));
        }
        LOG.info("+++++++++");
      }
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
      
      //for each query
      for(int qid : relLMMap.keySet()) {
        
        RelLM rellm = relLMMap.get(qid);
        
        double rsv = 0.0;
        
        int occurringTerms = 0;
        //for each term in the relevance LM
        for(int termid : rellm.probMap.keySet()) {
          double pwq = rellm.getWeight(termid);
          
          double tf = 0.0;
          if(tfMap.containsKey(termid)) {
            tf = tfMap.get(termid);
          }
          
          if(tf>0) {
            occurringTerms++;
          }
          
          double df = stats.getDf(termid);
          double mlProb = tf / (double) DOC.getLength();
          double colProb = df / (double) stats.getCollectionSize();

          double pwd = 0.0;

          // JM smoothing
          if (smoothingParam <= 1.0) {
            pwd = smoothingParam * mlProb + (1.0 - smoothingParam) * colProb;
          }
          // Dirichlet smoothing
          else {
            pwd = (double) (tf + smoothingParam * colProb)
                / (double) (DOC.getLength() + smoothingParam);
          }
          
          rsv += pwq * Math.log(pwq/pwd);
        }
        
        rsv = -1 * rsv;//the lower the KL divergence between relLM and docLM, the better
        
        if(occurringTerms>0) {
          keyOut.set(qid, key.toString());//qid,docid
          valueOut.set((float)rsv);
          context.write(keyOut,valueOut);
        }
      }
    }
  }

  private static class MyReducer extends
      Reducer<PairOfIntString, FloatWritable, NullWritable, Text> {

    private int topk;
    // PairOfStringFloat is (docid,score)
    private Map<Integer, PriorityQueue<PairOfStringFloat>> queueMap;
    private static final NullWritable nullKey = NullWritable.get();
    private static final Text valueOut = new Text();

    public void setup(Context context) throws IOException {

      topk = context.getConfiguration().getInt(TOPK, 1000);
      LOG.info("topk parameter set to " + topk);
      queueMap = Maps.newHashMap();
    }

    @Override
    public void reduce(PairOfIntString key, Iterable<FloatWritable> values, Context context)
        throws IOException, InterruptedException {

      int qid = key.getLeftElement();

      PriorityQueue<PairOfStringFloat> queue = null;

      if (queueMap.containsKey(qid)) {
        queue = queueMap.get(qid);
      } else {
        queue = new PriorityQueue<PairOfStringFloat>(topk + 1, new PairOfStringFloatComparator());
        queueMap.put(qid, queue);
      }

      //should only contain 1 value
      float scoreSum = 0f;
      for (FloatWritable v : values) {
        scoreSum += v.get();
      }

      // if there are less than topk elements, add the new (docid,score)
      // to the queue
      if (queue.size() < topk) {
        queue.add(new PairOfStringFloat(key.getRightElement(), scoreSum));
      }
      // if we have topk elements in the queue, we need to check if the
      // queue's current minimum is smaller than
      // the incoming score; if yes, "exchnage" the (docid,score) elements
      else if (queue.peek().getRightElement() < scoreSum) {
        queue.remove();
        queue.add(new PairOfStringFloat(key.getRightElement(), scoreSum));
      }
    }

    // emit the scores for all queries
    public void cleanup(Context context) throws IOException, InterruptedException {

      for (int qid : queueMap.keySet()) {
        PriorityQueue<PairOfStringFloat> queue = queueMap.get(qid);

        if (queue.size() == 0) {
          continue;
        }

        List<PairOfStringFloat> orderedList = new ArrayList<PairOfStringFloat>();
        while (queue.size() > 0) {
          orderedList.add(queue.remove());
        }

        for (int i = orderedList.size(); i > 0; i--) {
          PairOfStringFloat p = orderedList.get(i - 1);
          valueOut.set(qid + " Q0 " + p.getLeftElement() + " " + (orderedList.size() - i + 1) + " "
              + p.getRightElement() + " lmretrieval");
          context.write(nullKey, valueOut);
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
  public static final String RMMODEL = "rmmodel";
  public static final String QUERY_LAMBDA = "queryLambda";
  
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
    options.addOption(OptionBuilder.withArgName("path").hasArg().withDescription("queries")
        .create(QUERIES_OPTION));
    options.addOption(OptionBuilder.withArgName("float").hasArg().withDescription("smoothing")
        .create(SMOOTHING));
    options.addOption(OptionBuilder.withArgName("int").hasArg().withDescription("topk")
        .create(TOPK));
    options.addOption(OptionBuilder.withArgName("string " + AnalyzerFactory.getOptions()).hasArg()
        .withDescription("preprocessing").create(PREPROCESSING));
    options.addOption(OptionBuilder.withArgName("path").hasArg().withDescription("rmmodel file")
        .create(RMMODEL));
    options.addOption(OptionBuilder.withArgName("float").hasArg().withDescription("queryLambda")
        .create(QUERY_LAMBDA));

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
        || !cmdline.hasOption(QUERY_LAMBDA)
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
    String rmmodel = cmdline.getOptionValue(RMMODEL);
    String queryLambda = cmdline.getOptionValue(QUERY_LAMBDA);

    LOG.info("Tool name: " + RMRetrieval.class.getSimpleName());
    LOG.info(" - docvector: " + docvector);
    LOG.info(" - output: " + output);
    LOG.info(" - dictionary: " + dictionary);
    LOG.info(" - queries: " + queries);
    LOG.info(" - smoothing: " + smoothing);
    LOG.info(" - topk: " + topk);
    LOG.info(" - preprocessing: " + preprocessing);
    LOG.info(" - rmmodel: " + rmmodel);
    LOG.info(" - queryLambda: " + queryLambda);

    Configuration conf = getConf();
    conf.set(DICTIONARY_OPTION, dictionary);
    conf.set(QUERIES_OPTION, queries);
    conf.setFloat(SMOOTHING, Float.parseFloat(smoothing));
    conf.setInt(TOPK, Integer.parseInt(topk));
    conf.set(PREPROCESSING, preprocessing);
    conf.set(RMMODEL, rmmodel);
    conf.setFloat(QUERY_LAMBDA, Float.parseFloat(queryLambda));

    conf.set("mapreduce.map.memory.mb", "10048");
    conf.set("mapreduce.map.java.opts", "-Xmx10048m");
    conf.set("mapreduce.reduce.memory.mb", "10048");
    conf.set("mapreduce.reduce.java.opts", "-Xmx10048m");
    conf.set("mapred.task.timeout", "6000000");// default is 600000

    FileSystem fs = FileSystem.get(conf);
    if (fs.exists(new Path(output)))
      fs.delete(new Path(output));

    Job job = new Job(conf, RMRetrieval.class.getSimpleName() + ":" + docvector);
    job.setJarByClass(RMRetrieval.class);

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
    LOG.info("Running " + RMRetrieval.class.getCanonicalName() + " with args "
        + Arrays.toString(args));
    ToolRunner.run(new RMRetrieval(), args);
  }
}
