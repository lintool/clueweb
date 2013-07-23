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
import org.clueweb.dictionary.PorterAnalyzer;

import tl.lin.data.array.IntArrayWritable;
import tl.lin.data.pair.PairOfIntString;
import tl.lin.data.pair.PairOfStringFloat;
import tl.lin.lucene.AnalyzerUtils;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

public class LMRetrieval extends Configured implements Tool {

	private static final Logger LOG = Logger.getLogger(LMRetrieval.class);

	/*
	 * Partitioner: all keys with the same qid go to the same reducer
	 */
	private static class MyPartitioner extends
			Partitioner<PairOfIntString, FloatWritable> {

		@Override
		public int getPartition(PairOfIntString arg0, FloatWritable arg1,
				int numPartitions) {
			return arg0.getLeftElement() % numPartitions;
		}
	}

	/*
	 * comparator for the priority queue: elements (docid,score) are sorted by
	 * score
	 */
	private static class CustomComparator implements
			Comparator<PairOfStringFloat> {
		@Override
		public int compare(PairOfStringFloat o1, PairOfStringFloat o2) {

			if (o1.getRightElement() == o2.getRightElement()) {
				return 0;
			}
			if (o1.getRightElement() > o2.getRightElement()) {
				return 1;
			}
			return -1;
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
		// private static final Analyzer ANALYZER = new
		// StandardAnalyzer(Version.LUCENE_43);
		private static final Analyzer ANALYZER = new PorterAnalyzer();

		/*
		 * for quick access store the queries in two hashmaps: 1. key: termid,
		 * value: list of queries in which the termid occurs 2. key: qid, value:
		 * list of termids that occur in the query
		 */
		private HashMap<Integer, HashSet<Integer>> termidQuerySet;
		private HashMap<Integer, HashSet<Integer>> queryTermidSet;

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
			LOG.info("Smoothing set to "+smoothingParam);

			// read the queries from file
			termidQuerySet = Maps.newHashMap();
			queryTermidSet = Maps.newHashMap();
			FSDataInputStream fsin = fs.open(new Path(context
					.getConfiguration().get(QUERIES_OPTION)));
			BufferedReader br = new BufferedReader(new InputStreamReader(fsin));
			String line;
			while ((line = br.readLine()) != null) {
				int index = line.indexOf(':');
				if (index < 0) {
					LOG.info("Query file line in incorrect format, expecting <num>:<term> <term>...\n,instead got:\n"
							+ line);
					continue;
				}
				int qid = Integer.parseInt(line.substring(0, index));
				HashSet<Integer> termidSet = Sets.newHashSet();

				// normalize the terms (same way as the documents)
				for (String term : AnalyzerUtils.parse(ANALYZER,
						line.substring(index + 1))) {

					int termid = dictionary.getId(term);

					if (termid < 0) {
						LOG.info("Query " + qid + ": term [" + term
								+ "] not found in the provided dictionary!");
						continue;
					}

					termidSet.add(termid);

					if (termidQuerySet.containsKey(termid)) {
						termidQuerySet.get(termid).add(qid);
					} else {
						Set<Integer> qids = Sets.newHashSet();
						qids.add(qid);
						termidQuerySet.put(termid, (HashSet<Integer>) qids);
					}
				}

				queryTermidSet.put(qid, termidSet);
			}
			fsin.close();
			br.close();
		}

		@Override
		public void map(Text key, IntArrayWritable ints, Context context)
				throws IOException, InterruptedException {

			PForDocVector.fromIntArrayWritable(ints, DOC);

			// determine which queries we care about for this document
			HashSet<Integer> queriesToDo = Sets.newHashSet();

			// tfMap of the document
			HashMap<Integer, Integer> tfMap = Maps.newHashMap();
			for (int termid : DOC.getTermIds()) {
				int tf = 1;
				if (tfMap.containsKey(termid))
					tf += tfMap.get(termid);
				tfMap.put(termid, tf);

				if (termidQuerySet.containsKey(termid)) {
					for (int qid : termidQuerySet.get(termid)) {
						queriesToDo.add(qid);
					}
				}
			}

			// for each of the interesting queries, compute log(P(q|d))
			for (int qid : queriesToDo) {
				double score = 0.0;

				for (int termid : queryTermidSet.get(qid)) {
					double tf = 0.0;
					if (tfMap.containsKey(termid))
						tf = tfMap.get(termid);
					double df = stats.getDf(termid);

					double mlProb = tf / (double) DOC.getLength();
					double colProb = df / (double) stats.getCollectionSize();

					double prob = 0.0;

					// JM smoothing
					if (smoothingParam <= 1.0) {
						prob = smoothingParam * mlProb + (1.0 - smoothingParam)
								* colProb;
					}
					// Dirichlet smoothing
					else {
						prob = (double) (tf + smoothingParam * colProb)
								/ (double) (DOC.getLength() + smoothingParam);
					}

					score += (float) Math.log(prob);
				}

				keyOut.set(qid, key.toString());
				valueOut.set((float) score);
				context.write(keyOut, valueOut);
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
			LOG.info("topk parameter set to "+topk);

			queueMap = Maps.newHashMap();
		}

		@Override
		public void reduce(PairOfIntString key, Iterable<FloatWritable> values,
				Context context) throws IOException, InterruptedException {

			int qid = key.getLeftElement();

			PriorityQueue<PairOfStringFloat> queue = null;

			if (queueMap.containsKey(qid)) {
				queue = queueMap.get(qid);
			} else {
				queue = new PriorityQueue<PairOfStringFloat>(topk + 1,
						new CustomComparator());
				queueMap.put(qid, queue);
			}

			// actually, it should only be a single element
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
		public void cleanup(Context context) throws IOException,
				InterruptedException {

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
					valueOut.set(qid + " Q0 " + p.getLeftElement() + " "
							+ (orderedList.size() - i + 1) + " "
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

	/**
	 * Runs this tool.
	 */
	@SuppressWarnings({ "static-access", "deprecation" })
	public int run(String[] args) throws Exception {
		Options options = new Options();

		options.addOption(OptionBuilder
				.withArgName("path")
				.hasArg()
				.withDescription(
						"input path (pfor format expected, add seg*/part*)")
				.create(DOCVECTOR_OPTION));
		options.addOption(OptionBuilder.withArgName("path").hasArg()
				.withDescription("output path").create(OUTPUT_OPTION));
		options.addOption(OptionBuilder.withArgName("path").hasArg()
				.withDescription("dictionary").create(DICTIONARY_OPTION));
		options.addOption(OptionBuilder.withArgName("path").hasArg()
				.withDescription("queries").create(QUERIES_OPTION));
		options.addOption(OptionBuilder.withArgName("float").hasArg()
				.withDescription("smoothing").create(SMOOTHING));
		options.addOption(OptionBuilder.withArgName("int").hasArg()
				.withDescription("topk").create(TOPK));

		CommandLine cmdline;
		CommandLineParser parser = new GnuParser();
		try {
			cmdline = parser.parse(options, args);
		} catch (ParseException exp) {
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp(this.getClass().getName(), options);
			ToolRunner.printGenericCommandUsage(System.out);
			System.err.println("Error parsing command line: "
					+ exp.getMessage());
			return -1;
		}

		if (!cmdline.hasOption(DOCVECTOR_OPTION)
				|| !cmdline.hasOption(OUTPUT_OPTION)
				|| !cmdline.hasOption(DICTIONARY_OPTION)
				|| !cmdline.hasOption(QUERIES_OPTION)
				|| !cmdline.hasOption(SMOOTHING) || !cmdline.hasOption(TOPK)) {
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

		LOG.info("Tool name: " + LMRetrieval.class.getSimpleName());
		LOG.info(" - docvector: " + docvector);
		LOG.info(" - output: " + output);
		LOG.info(" - dictionary: " + dictionary);
		LOG.info(" - queries: " + queries);
		LOG.info(" - smoothing: " + smoothing);
		LOG.info(" - topk: " + topk);

		Configuration conf = getConf();
		conf.set(DICTIONARY_OPTION, dictionary);
		conf.set(QUERIES_OPTION, queries);
		conf.setFloat(SMOOTHING, Float.parseFloat(smoothing));
		conf.setInt(TOPK, Integer.parseInt(topk));

		conf.set("mapreduce.map.memory.mb", "10048");
		conf.set("mapreduce.map.java.opts", "-Xmx10048m");
		conf.set("mapreduce.reduce.memory.mb", "10048");
		conf.set("mapreduce.reduce.java.opts", "-Xmx10048m");
		conf.set("mapred.task.timeout", "6000000");// default is 600000

		FileSystem fs = FileSystem.get(conf);
		if (fs.exists(new Path(output)))
			fs.delete(new Path(output));

		Job job = new Job(conf, LMRetrieval.class.getSimpleName() + ":"
				+ docvector);
		job.setJarByClass(LMRetrieval.class);

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
		LOG.info("Job Finished in " + (System.currentTimeMillis() - startTime)
				/ 1000.0 + " seconds");
		return 0;
	}

	/**
	 * Dispatches command-line arguments to the tool via the
	 * <code>ToolRunner</code>.
	 */
	public static void main(String[] args) throws Exception {
		LOG.info("Running " + LMRetrieval.class.getCanonicalName()
				+ " with args " + Arrays.toString(args));
		ToolRunner.run(new LMRetrieval(), args);
	}
}
