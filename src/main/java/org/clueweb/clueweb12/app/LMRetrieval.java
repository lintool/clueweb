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
 * @author: Claudia
 * 
 * Implementation of language modeling. Retrieval parameter <i>smoothing</i> determines the type: 
 * 		smoothing<=1 means Jelineck-Mercer and smoothing>1 means Dirichlet.
 * 
 * Approach:
 * 
 * (1) read the queries and convert into termids (based on the dictionary)
 * (2) MyMapper: walk over all document vectors and as soon as a termid is found which occurs in a query,
 * 		output: key:qid,value:(docid,score)
 * (3) MyReducer: all scores for one query appear at a single reduce() step; sum up the scores for each docid and write out the topk docids
 * 		with the highest scores - a priority queue ensures that only the topk remain, but within this interval they remain unsorted
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
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.clueweb.dictionary.DefaultFrequencySortedDictionary;
import org.clueweb.data.TermStatistics;
import org.clueweb.data.VByteDocVector;

import tl.lin.data.pair.PairOfIntLong;
import tl.lin.data.pair.PairOfStringFloat;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

public class LMRetrieval extends Configured implements Tool {
	private static final Logger LOG = Logger.getLogger(LMRetrieval.class);

	/*
	 * Mapper outKey: query id (integer) Mapper outValue: two part element:
	 * documentid probability
	 */
	private static class MyMapper
			extends
			Mapper<Text, BytesWritable, IntWritable, tl.lin.data.pair.PairOfStringFloat> {
		private static final VByteDocVector DOC = new VByteDocVector();

		private DefaultFrequencySortedDictionary dictionary;
		private TermStatistics stats;

		private double smoothing;

		// key: termid, hashset value: qids where the termid occurs in
		private HashMap<Integer, HashSet<Integer>> termidQuerySet;

		private static final IntWritable keyOut = new IntWritable();// qid
		private static final PairOfStringFloat valueOut = new PairOfStringFloat();// (docid,score)

		@Override
		public void setup(Context context) throws IOException {
			FileSystem fs = FileSystem.get(context.getConfiguration());
			String path = context.getConfiguration().get(DICTIONARY_OPTION);
			dictionary = new DefaultFrequencySortedDictionary(path, fs);
			path = context.getConfiguration().get(COLLECTION_STATS);
			stats = new TermStatistics(new Path(path), fs);

			try {
				smoothing = Double.parseDouble(context.getConfiguration().get(
						SMOOTHING));
			} catch (NumberFormatException e) {
				LOG.info("Smoothing is expected to parse into a double, found: "
						+ context.getConfiguration().get(SMOOTHING));
				LOG.info("Smoothing set to 1000 (thus Dirichlet smoothing) due to an error in parsing!");
				smoothing = 1000;
			}

			// read the queries from file
			termidQuerySet = Maps.newHashMap();
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
				String terms[] = line.substring(index + 1).split("\\s");

				for (String term : terms) {
					int termid = dictionary.getId(term);
					if (termid < 0) {
						LOG.info("Query " + qid + ": term [" + term
								+ "] not found in the provided dictionary!");
						continue;
					}

					if (termidQuerySet.containsKey(termid))
						termidQuerySet.get(termid).add(qid);
					else {
						HashSet<Integer> qids = Sets.newHashSet();
						qids.add(qid);
						termidQuerySet.put(termid, qids);
					}
				}
			}
			fsin.close();
			br.close();
		}

		@Override
		public void map(Text key, BytesWritable bytes, Context context)
				throws IOException, InterruptedException {

			VByteDocVector.fromBytesWritable(bytes, DOC);

			// tfMap of the document
			HashMap<Integer, Integer> tfMap = Maps.newHashMap();
			for (int termid : DOC.getTermIds()) {
				int tf = 1;
				if (tfMap.containsKey(termid))
					tf += tfMap.get(termid);
				tfMap.put(termid, tf);
			}

			// walk over all found document terms and check if one of them is a
			// query term
			for (int termid : tfMap.keySet()) {

				// if not, go on
				if (!termidQuerySet.containsKey(termid))
					continue;

				double tf = tfMap.get(termid);
				double df = stats.getDf(termid);

				double mlProb = tf / (double) DOC.getLength();
				double colProb = df / (double) stats.getCollectionSize();

				double prob = 0.0;

				// JM smoothing
				if (smoothing <= 1.0)
					prob = smoothing * mlProb + (1.0 - smoothing) * colProb;
				// Dirichlet smoothing
				else
					prob = (double) (tf + smoothing * colProb)
							/ (double) (DOC.getLength() + smoothing);

				prob = Math.log(prob);

				HashSet<Integer> queries = termidQuerySet.get(termid);
				for (Integer query : queries) {
					keyOut.set(query);
					valueOut.set(key.toString(), (float) prob);// remember: key
																// is docid
					context.write(keyOut, valueOut);
				}
			}
		}
	}

	// key: qid, value: (docid,score)
	private static class MyCombiner
			extends
			Reducer<IntWritable, PairOfStringFloat, IntWritable, PairOfStringFloat> {
		private static final PairOfStringFloat output = new PairOfStringFloat();

		@Override
		public void reduce(IntWritable key, Iterable<PairOfStringFloat> values,
				Context context) throws IOException, InterruptedException {

			HashMap<String, Float> docProbs = Maps.newHashMap();
			for (PairOfStringFloat v : values) 
			{
				String docid = v.getLeftElement();
				float logProb = v.getRightElement();
				if (docProbs.containsKey(docid))
					logProb += docProbs.get(docid);
				docProbs.put(docid, logProb);
			
				if(docProbs.size()>100000)
				{
					for(String d : docProbs.keySet())
					{
						output.set(d, docProbs.get(d));
						context.write(key, output);
					}
					docProbs.clear();
				}
			}
		}
	}

	private static class MyReducer extends
			Reducer<IntWritable, PairOfStringFloat, IntWritable, Text> {
		private static final Text valueOut = new Text();

		private int topk;

		public void setup(Context context) throws IOException {
			try {
				topk = Integer.parseInt(context.getConfiguration().get(TOPK));
			} catch (NumberFormatException e) {
				LOG.info("topK should parse into an Integer, instead is "
						+ context.getConfiguration().get(TOPK));
				LOG.info("Setting topK to 10000");
				topk = 10000;
			}
		}

		@Override
		public void reduce(IntWritable key, Iterable<PairOfStringFloat> values,
				Context context) throws IOException, InterruptedException {

			// key: qid
			// value: <docid,prob>

			// in-memory hack: keep a hashmap of all docids and aggregate the
			// log probs
			HashMap<String, Double> docProbs = Maps.newHashMap();
			for (PairOfStringFloat v : values) {
				String docid = v.getLeftElement();
				double logProb = v.getRightElement();
				if (docProbs.containsKey(docid))
					logProb += docProbs.get(docid);
				docProbs.put(docid, logProb);
			}

			// keep the top-k
/*			PriorityQueue<Double> pq = new PriorityQueue<Double>(topk+1);
			for (Double d : docProbs.values()) {
				pq.add(d);
				if (pq.size() > topk)
					pq.remove();
			}

			// what is left at the head of the queue?
			double minVal = pq.remove();
*/
			// output everything that has a score of minVal or larger
			for (String docid : docProbs.keySet()) {
//				if (docProbs.get(docid) < minVal)
//					continue;
				valueOut.set(docid + "\t" + docProbs.get(docid));
				context.write(key, valueOut);
			}
		}
	}

	public static final String VBDOCVECTOR_OPTION = "vbdocvector";
	public static final String OUTPUT_OPTION = "output";
	public static final String DICTIONARY_OPTION = "dictionary";
	public static final String QUERIES_OPTION = "queries";
	public static final String COLLECTION_STATS = "colstats";
	public static final String SMOOTHING = "smoothing";
	public static final String TOPK = "topk";

	/**
	 * Runs this tool.
	 */
	@SuppressWarnings({ "static-access", "deprecation" })
	public int run(String[] args) throws Exception {
		Options options = new Options();

		options.addOption(OptionBuilder.withArgName("path").hasArg()
				.withDescription("input path").create(VBDOCVECTOR_OPTION));
		options.addOption(OptionBuilder.withArgName("path").hasArg()
				.withDescription("output path").create(OUTPUT_OPTION));
		options.addOption(OptionBuilder.withArgName("path").hasArg()
				.withDescription("dictionary").create(DICTIONARY_OPTION));
		options.addOption(OptionBuilder.withArgName("path").hasArg()
				.withDescription("queries").create(QUERIES_OPTION));
		options.addOption(OptionBuilder.withArgName("path").hasArg()
				.withDescription("colstats").create(COLLECTION_STATS));
		options.addOption(OptionBuilder.withArgName("double").hasArg()
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

		if (!cmdline.hasOption(VBDOCVECTOR_OPTION)
				|| !cmdline.hasOption(OUTPUT_OPTION)
				|| !cmdline.hasOption(DICTIONARY_OPTION)
				|| !cmdline.hasOption(QUERIES_OPTION)
				|| !cmdline.hasOption(COLLECTION_STATS)
				|| !cmdline.hasOption(SMOOTHING) || !cmdline.hasOption(TOPK)) {
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp(this.getClass().getName(), options);
			ToolRunner.printGenericCommandUsage(System.out);
			return -1;
		}

		String vbdocvector = cmdline.getOptionValue(VBDOCVECTOR_OPTION);
		String output = cmdline.getOptionValue(OUTPUT_OPTION);
		String dictionary = cmdline.getOptionValue(DICTIONARY_OPTION);
		String queries = cmdline.getOptionValue(QUERIES_OPTION);
		String colstats = cmdline.getOptionValue(COLLECTION_STATS);
		String smoothing = cmdline.getOptionValue(SMOOTHING);
		String topk = cmdline.getOptionValue(TOPK);

		LOG.info("Tool name: " + LMRetrieval.class.getSimpleName());
		LOG.info(" - vbdocvector: " + vbdocvector);
		LOG.info(" - output: " + output);
		LOG.info(" - dictionary: " + dictionary);
		LOG.info(" - queries: " + queries);
		LOG.info(" - colstats: " + colstats);
		LOG.info(" - smoothing: " + smoothing);
		LOG.info(" - topk: " + topk);

		Configuration conf = getConf();
		conf.set(DICTIONARY_OPTION, dictionary);
		conf.set(QUERIES_OPTION, queries);
		conf.set(COLLECTION_STATS, colstats);
		conf.set(SMOOTHING, smoothing);
		conf.set(TOPK, topk);
		
	    conf.set("mapreduce.map.memory.mb", "10048");
	    conf.set("mapreduce.map.java.opts", "-Xmx10048m");
	    conf.set("mapreduce.reduce.memory.mb", "10048");
	    conf.set("mapreduce.reduce.java.opts", "-Xmx10048m");
	    conf.set("mapred.task.timeout", "600000");

		FileSystem fs = FileSystem.get(conf);
		if(fs.exists(new Path(output)))
			fs.delete(new Path(output));

	    
		Job job = new Job(conf, LMRetrieval.class.getSimpleName() + ":"
				+ vbdocvector);
		job.setJarByClass(LMRetrieval.class);

		FileInputFormat.setInputPaths(job, vbdocvector);
		FileOutputFormat.setOutputPath(job, new Path(output));

		job.setInputFormatClass(SequenceFileInputFormat.class);

		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(PairOfStringFloat.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(MyMapper.class);
//		job.setCombinerClass(MyCombiner.class);
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
