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
 * 2. in setup() read the top numFeedbackDocs docids per query and compute the weight of each document
 * 
 * 3. in the map() walk over all documents; if a document is hit that is among the top results,
 * compute P(w|D)xdocWeight and emit (query,(term,weight))
 * 
 * 4. in the reducer, add up all (term,weight) pairs and keep the top m; write those out to file
 * 			query term P(w|R)
 */

package org.clueweb.clueweb12.app;

import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
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
import org.clueweb.dictionary.DefaultFrequencySortedDictionary;
import org.clueweb.util.TRECResult;
import org.clueweb.util.TRECResultFileParser;

import tl.lin.data.array.IntArrayWritable;
import tl.lin.data.pair.PairOfIntString;
import tl.lin.data.pair.PairOfStringFloat;
import com.google.common.collect.Maps;

public class RMModel extends Configured implements Tool {

	private static final Logger LOG = Logger.getLogger(RMModel.class);

	/*
	 * Mapper outKey: (qid,docid), value: probability score
	 */
	private static class MyMapper extends
			Mapper<Text, IntArrayWritable, IntWritable, PairOfStringFloat> {

		private static final PForDocVector DOC = new PForDocVector();
		private DefaultFrequencySortedDictionary dictionary;

		// complex key: (qid,docid)
		private static final IntWritable keyOut = new IntWritable();
		// value: float; probability score log(P(q|d))
		private static final PairOfStringFloat valueOut = new PairOfStringFloat();

		// outer key: docid, inner key: qid, inner value: weight
		private static HashMap<String, HashMap<Integer, Double>> docidWeights;

		@Override
		public void setup(Context context) throws IOException {

			FileSystem fs = FileSystem.get(context.getConfiguration());
			String path = context.getConfiguration().get(DICTIONARY_OPTION);
			dictionary = new DefaultFrequencySortedDictionary(path, fs);

			docidWeights = Maps.newHashMap();

			int numFeedbackDocs = context.getConfiguration().getInt(
					NUM_FEEDBACK_DOCS, 20);
			int numFeedbackTerms = context.getConfiguration().getInt(
					NUM_FEEDBACK_TERMS, 20);

			LOG.info("Number of feedback documents set to: " + numFeedbackDocs);
			LOG.info("Number of feedback terms set set to: " + numFeedbackTerms);

			// read the TREC result file of the initial retrieval run as input
			// TREC result file is expected to be in order (higher ranked
			// documents first)
			// computed are for each (qid,did) the normalized weight of the
			// document
			TRECResultFileParser parser = new TRECResultFileParser(fs,
					new Path(context.getConfiguration().get(TREC_RESULT_FILE)));

			int currentQuery = -1;
			// key: docid, value: retrieval score
			HashMap<String, Double> scores = Maps.newHashMap();
			while (parser.hasNext()) {
				TRECResult tr = parser.getNext();
				tr.score = Math.exp(tr.score);

				if (currentQuery == tr.qid && scores.size() == numFeedbackDocs) {
					double sum = 0.0;
					for (String s : scores.keySet()) {
						sum += scores.get(s);
					}

					for (String s : scores.keySet()) {
						HashMap<Integer, Double> m = null;
						if (docidWeights.containsKey(s)) {
							m = docidWeights.get(s);
						} else {
							m = Maps.newHashMap();
							docidWeights.put(s, m);
						}
						m.put(tr.qid, scores.get(s) / sum);
					}
				} else if (currentQuery != tr.qid) {
					currentQuery = tr.qid;
					scores.clear();
				} else {
					;
				}
				scores.put(tr.did, tr.score);
			}
		}

		@Override
		public void map(Text key, IntArrayWritable ints, Context context)
				throws IOException, InterruptedException {

			PForDocVector.fromIntArrayWritable(ints, DOC);

			// are we interested in this document?
			if (docidWeights.containsKey(key.toString()) == false) {
				return;
			}

			// tfMap of the document
			HashMap<Integer, Integer> tfMap = Maps.newHashMap();
			for (int termid : DOC.getTermIds()) {
				int tf = 1;
				if (tfMap.containsKey(termid))
					tf += tfMap.get(termid);
				tfMap.put(termid, tf);
			}

			// compute P(term|doc) for each term occurring in the doc
			for (Integer termid : tfMap.keySet()) {

				double mlProb = (double) tfMap.get(termid)
						/ (double) DOC.getLength();

				// for all queries, in which this document appears, emit
				// something
				HashMap<Integer, Double> normScores = docidWeights.get(key
						.toString());

				for (int qid : normScores.keySet()) {
					double weight = normScores.get(qid);

					double p = weight * mlProb;

					keyOut.set(qid);
					valueOut.set(dictionary.getTerm(termid), (float) p);
					context.write(keyOut, valueOut);
				}
			}
		}
	}

	private static class MyReducer extends
			Reducer<IntWritable, PairOfStringFloat, NullWritable, Text> {

		private HashMap<String, Double> termMap;
		private static final NullWritable nullKey = NullWritable.get();
		private static final Text valueOut = new Text();

		public void setup(Context context) throws IOException {
			termMap = Maps.newHashMap();
		}

		@Override
		public void reduce(IntWritable key, Iterable<PairOfStringFloat> values,
				Context context) throws IOException, InterruptedException {

			for (PairOfStringFloat p : values) {
				String term = p.getLeftElement();
				float score = p.getRightElement();

				if (termMap.containsKey(term)) {
					termMap.put(term, termMap.get(term) + score);
				} else {
					termMap.put(term, (double) score);
				}
			}

			StringBuffer sb = new StringBuffer();
			sb.append("++++ " + key.get() + " ++++\n");

			PriorityQueue<PairOfStringFloat> queue = new PriorityQueue<PairOfStringFloat>(
					20);

			for (String term : termMap.keySet()) {
				if (queue.size() < 20) {
					PairOfStringFloat p = new PairOfStringFloat();
					p.set(term, termMap.get(term).floatValue());
					queue.add(p);
				} else {
					if (queue.peek().getRightElement() < termMap.get(term)) {
						queue.remove();
						PairOfStringFloat p = new PairOfStringFloat();
						p.set(term, termMap.get(term).floatValue());
						queue.add(p);
					}
				}
			}

			while (queue.size() > 0) {
				PairOfStringFloat p = queue.remove();
				sb.append(p.getLeftElement() + " " + p.getRightElement());
			}
			
			valueOut.set(sb.toString());
			context.write(nullKey, valueOut);
		}
	}

	public static final String DOCVECTOR_OPTION = "docvector";
	public static final String OUTPUT_OPTION = "output";
	public static final String NUM_FEEDBACK_DOCS = "numFeedbackDocs";
	public static final String NUM_FEEDBACK_TERMS = "numFeedbackTerms";
	public static final String TREC_RESULT_FILE = "trecinputfile";
	public static final String DICTIONARY_OPTION = "dictionary";

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
						"input path (pfor format expected, add * to retrieve files)")
				.create(DOCVECTOR_OPTION));
		options.addOption(OptionBuilder.withArgName("path").hasArg()
				.withDescription("output path").create(OUTPUT_OPTION));
		options.addOption(OptionBuilder.withArgName("path").hasArg()
				.withDescription("dictionary").create(DICTIONARY_OPTION));
		options.addOption(OptionBuilder.withArgName("int").hasArg()
				.withDescription("numFeedbackDocs").create(NUM_FEEDBACK_DOCS));
		options.addOption(OptionBuilder.withArgName("int").hasArg()
				.withDescription("numFeedbackTerms").create(NUM_FEEDBACK_TERMS));
		options.addOption(OptionBuilder.withArgName("path").hasArg()
				.withDescription("input path").create(TREC_RESULT_FILE));

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
				|| !cmdline.hasOption(TREC_RESULT_FILE)
				|| !cmdline.hasOption(NUM_FEEDBACK_DOCS)
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
		int numDocs = Integer.parseInt(cmdline
				.getOptionValue(NUM_FEEDBACK_DOCS));
		int numTerms = Integer.parseInt(cmdline
				.getOptionValue(NUM_FEEDBACK_TERMS));

		LOG.info("Tool name: " + RMModel.class.getSimpleName());
		LOG.info(" - docvector: " + docvector);
		LOG.info(" - output: " + output);
		LOG.info(" - dictionary: " + dictionary);
		LOG.info(" - trecinputfile: " + trecinput);
		LOG.info(" - numFeedbackDocs: " + numDocs);
		LOG.info(" - numFeedbackTerms: " + numTerms);

		Configuration conf = getConf();
		conf.set(DICTIONARY_OPTION, dictionary);
		conf.set(TREC_RESULT_FILE, trecinput);
		conf.setInt(NUM_FEEDBACK_DOCS, numDocs);
		conf.setInt(NUM_FEEDBACK_TERMS, numTerms);

		FileSystem fs = FileSystem.get(conf);
		if (fs.exists(new Path(output)))
			fs.delete(new Path(output));

		Job job = new Job(conf, RMModel.class.getSimpleName() + ":" + docvector);
		job.setJarByClass(RMModel.class);

		FileInputFormat.setInputPaths(job, docvector);
		FileOutputFormat.setOutputPath(job, new Path(output));

		job.setInputFormatClass(SequenceFileInputFormat.class);

		job.setMapOutputKeyClass(PairOfIntString.class);
		job.setMapOutputValueClass(FloatWritable.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(MyMapper.class);
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
		LOG.info("Running " + RMModel.class.getCanonicalName() + " with args "
				+ Arrays.toString(args));
		ToolRunner.run(new RMModel(), args);
	}
}
