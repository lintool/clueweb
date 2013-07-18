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
 * (1) read the queries and convert into termids (based on the dictionary)
 * (2) MyMapper: walk over all document vectors and as soon as a termid is found in a document which occurs in a query,
 * 		output: composite key (qid,docid) and value score
 * (3) MyPartitioner: ensures that all keys (qid,docid) with the same qid end up in the same reducer
 * 		TODO: sanity check => are the (qid,docid) objects sorted as expected?
 * (4) MyReducer: keep track of the current query and a priority queue (min-heap); for each (qid,docid) key, sum up all scores
 * 		and if the heap is not at maximum capacity or the current score is larger than the heap's current minimum, add the score to the heap;
 * 		once a new qid is encountered, write out what is in the heap (scores and docids) for that query; reset everything and process the next query
 * 
 * TODO: queries with quasi-stopwords slow down the process a lot
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
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;
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
import org.clueweb.data.TermStatistics;
import org.clueweb.data.VByteDocVector;
import org.clueweb.dictionary.DefaultFrequencySortedDictionary;

import tl.lin.data.pair.PairOfIntString;
import tl.lin.data.pair.PairOfStringFloat;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

public class LMRetrieval extends Configured implements Tool {
	
	private static final Logger LOG = Logger.getLogger(LMRetrieval.class);
	
	
	/*
	 * everything with the same qid goes to the same reducer
	 */
	private static class MyPartitioner extends Partitioner<PairOfIntString, FloatWritable> {

		@Override
		public int getPartition(PairOfIntString arg0, FloatWritable arg1,
				int numPartitions) {
			return arg0.getLeftElement()%numPartitions;
		}
	 
	}
	
	
	private static class CustomComparator implements Comparator<PairOfStringFloat>
	{
		@Override
		public int compare(PairOfStringFloat o1, PairOfStringFloat o2) {
			
			if(o1.getRightElement()==o2.getRightElement())
				return 0;
			if(o1.getRightElement()>o2.getRightElement())
				return 1;
			return -1;
		}
	}
	

	/*
	 * Mapper outKey: query id (integer) Mapper outValue: two part element:
	 * documentid probability
	 */
	private static class MyMapper extends
			Mapper<Text, BytesWritable, PairOfIntString, FloatWritable> {
		private static final VByteDocVector DOC = new VByteDocVector();

		private DefaultFrequencySortedDictionary dictionary;
		private TermStatistics stats;

		private double smoothing;

		// key: termid, hashset value: qids where the termid occurs in
		private HashMap<Integer, HashSet<Integer>> termidQuerySet;
		private HashMap<Integer, HashSet<Integer>> queryTermidSet;

		private static final PairOfIntString keyOut = new PairOfIntString();// qid
																			// and
																			// docid
		private static final FloatWritable valueOut = new FloatWritable();// score

		@Override
		public void setup(Context context) throws IOException {
			FileSystem fs = FileSystem.get(context.getConfiguration());
			String path = context.getConfiguration().get(DICTIONARY_OPTION);
			dictionary = new DefaultFrequencySortedDictionary(path, fs);
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
				
				String terms[] = line.substring(index + 1).split("\\s");

				for (String term : terms) {
					int termid = dictionary.getId(term);
					if (termid < 0) {
						LOG.info("Query " + qid + ": term [" + term
								+ "] not found in the provided dictionary!");
						continue;
					}
					
					termidSet.add(termid);

					if (termidQuerySet.containsKey(termid))
						termidQuerySet.get(termid).add(qid);
					else {
						HashSet<Integer> qids = Sets.newHashSet();
						qids.add(qid);
						termidQuerySet.put(termid, qids);
					}
				}
				
				queryTermidSet.put(qid, termidSet);
			}
			fsin.close();
			br.close();
		}

		@Override
		public void map(Text key, BytesWritable bytes, Context context)
				throws IOException, InterruptedException {

			VByteDocVector.fromBytesWritable(bytes, DOC);
			
			HashSet<Integer> queriesToDo = Sets.newHashSet();

			//tfMap of the document
			HashMap<Integer, Integer> tfMap = Maps.newHashMap();
			for (int termid : DOC.getTermIds()) {
				int tf = 1;
				if (tfMap.containsKey(termid))
					tf += tfMap.get(termid);
				tfMap.put(termid, tf);
				
				if(termidQuerySet.containsKey(termid))
				{
					for(int qid : termidQuerySet.get(termid))
						queriesToDo.add(qid);
				}
			}
			
			//we now know all queries for which term statistics need to be emitted
			for(int qid : queriesToDo)
			{
				for(int termid : queryTermidSet.get(qid))
				{
					double tf = 0f;
					if(tfMap.containsKey(termid))
						tf = tfMap.get(termid);
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

					keyOut.set(qid, key.toString());
					valueOut.set((float) prob);
					context.write(keyOut, valueOut);
				}
			}
		}
	}
	
	private static class MyCombiner extends
	Reducer<PairOfIntString, FloatWritable, PairOfIntString, FloatWritable> {
		
		private static final FloatWritable valueOut = new FloatWritable();
	
		public void reduce(PairOfIntString key, Iterable<FloatWritable> values,
				Context context) throws IOException, InterruptedException {
			
			float sum = 0f;
			for(FloatWritable v : values)
				sum += v.get();
			valueOut.set(sum);
			context.write(key, valueOut);
		}

	}


	private static class MyReducer extends
			Reducer<PairOfIntString, FloatWritable, NullWritable, Text> {

		private int topk;
		private PriorityQueue<PairOfStringFloat> queue;//docid score
		private int currentQuery;
		private static final NullWritable nullKey = NullWritable.get();
		private static final Text valueOut = new Text();

		public void setup(Context context) throws IOException {
			try {
				topk = Integer.parseInt(context.getConfiguration().get(TOPK));
			} catch (NumberFormatException e) {
				LOG.info("topK should parse into an Integer, instead is "
						+ context.getConfiguration().get(TOPK));
				topk = 1000;
				LOG.info("Setting topK to "+topk);
			}
			
			queue = new PriorityQueue<PairOfStringFloat>(topk+1,new CustomComparator());
			currentQuery = -1;
		}

		@Override
		public void reduce(PairOfIntString key, Iterable<FloatWritable> values,
				Context context) throws IOException, InterruptedException {

			// key: (qid,docid)
			// value: score

			if(key.getLeftElement()!=currentQuery)
			{
				//output what is currently in the priority queue
				if(currentQuery>0)
				{
					//convert minimum heap into list with ascending order
					List<PairOfStringFloat> orderedList = new ArrayList<PairOfStringFloat>();
					while(queue.size()>0)
						orderedList.add(queue.remove());
					
					//write out the list in TREC result file format
					for(int i=orderedList.size(); i>0; i--)
					{
						PairOfStringFloat p = orderedList.get(i-1);
						valueOut.set(currentQuery+" Q0 "+p.getLeftElement()+" "+(orderedList.size()-i+1)+" "+p.getRightElement()+" lmretrieval");
						context.write(nullKey, valueOut);
					}
				}
				
				//reset everything
				queue.clear();
				currentQuery = key.getLeftElement();
			}
			
			float scoreSum = 0f;
			for(FloatWritable v : values)
				scoreSum += v.get();
			
			//if there are less than topk elements, just add the current one
			if(queue.size()<topk)
				queue.add(new PairOfStringFloat(key.getRightElement(),scoreSum));
			//if we have topk elements in the queue, we need to check if the queue's current min. is smaller than
			//the incoming score; if yes, remove the current min. from the heap and add the score;
			//otherwise do nothing
			else if(queue.peek().getRightElement()<scoreSum)
			{
				queue.remove();
				queue.add(new PairOfStringFloat(key.getRightElement(),scoreSum));
			}
			else
				;
		}
		
		
		//don't forget the last query's content!
		public void cleanup(Context context) throws IOException, InterruptedException {

			if(currentQuery>0 && queue.size()>0)
			{
				List<PairOfStringFloat> orderedList = new ArrayList<PairOfStringFloat>();
				while(queue.size()>0)
					orderedList.add(queue.remove());

				for(int i=orderedList.size(); i>0; i--)
				{
					PairOfStringFloat p = orderedList.get(i-1);
					valueOut.set(currentQuery+" Q0 "+p.getLeftElement()+" "+(orderedList.size()-i+1)+" "+p.getRightElement()+" lmretrieval");
					context.write(nullKey, valueOut);
				}
			}	
		}
	}

	public static final String VBDOCVECTOR_OPTION = "vbdocvector";
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

		options.addOption(OptionBuilder.withArgName("path").hasArg()
				.withDescription("input path (seg*/part*)")
				.create(VBDOCVECTOR_OPTION));
		options.addOption(OptionBuilder.withArgName("path").hasArg()
				.withDescription("output path").create(OUTPUT_OPTION));
		options.addOption(OptionBuilder.withArgName("path").hasArg()
				.withDescription("dictionary").create(DICTIONARY_OPTION));
		options.addOption(OptionBuilder.withArgName("path").hasArg()
				.withDescription("queries").create(QUERIES_OPTION));
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
		String smoothing = cmdline.getOptionValue(SMOOTHING);
		String topk = cmdline.getOptionValue(TOPK);

		LOG.info("Tool name: " + LMRetrieval.class.getSimpleName());
		LOG.info(" - vbdocvector: " + vbdocvector);
		LOG.info(" - output: " + output);
		LOG.info(" - dictionary: " + dictionary);
		LOG.info(" - queries: " + queries);
		LOG.info(" - smoothing: " + smoothing);
		LOG.info(" - topk: " + topk);

		Configuration conf = getConf();
		conf.set(DICTIONARY_OPTION, dictionary);
		conf.set(QUERIES_OPTION, queries);
		conf.set(SMOOTHING, smoothing);
		conf.set(TOPK, topk);

		conf.set("mapreduce.map.memory.mb", "10048");
		conf.set("mapreduce.map.java.opts", "-Xmx10048m");
		conf.set("mapreduce.reduce.memory.mb", "10048");
		conf.set("mapreduce.reduce.java.opts", "-Xmx10048m");
		conf.set("mapred.task.timeout", "6000000");//default is 600000

		FileSystem fs = FileSystem.get(conf);
		if (fs.exists(new Path(output)))
			fs.delete(new Path(output));

		Job job = new Job(conf, LMRetrieval.class.getSimpleName() + ":"
				+ vbdocvector);
		job.setJarByClass(LMRetrieval.class);

		FileInputFormat.setInputPaths(job, vbdocvector);
		FileOutputFormat.setOutputPath(job, new Path(output));

		job.setInputFormatClass(SequenceFileInputFormat.class);

		job.setMapOutputKeyClass(PairOfIntString.class);
		job.setMapOutputValueClass(FloatWritable.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(MyMapper.class);
		job.setPartitionerClass(MyPartitioner.class);
		job.setCombinerClass(MyCombiner.class);
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
