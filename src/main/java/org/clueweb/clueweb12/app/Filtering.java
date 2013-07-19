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
 * Implementation of near-duplicate filtering: given a TREC result file as input, each document in the ranking
 * is compared with all documents ranked before it - if it has a cosine similarity > X to one of them, it is discarded as duplicate.
 * Note: no fancy similarity based in hashes of hashes, just TF.IDF weighted cosine similarity
 * 
 * Approach:
 * 
 * (1) read a TREC result file as input and keep track of (qid, rank, docid)
 * 
 * (2) MyMapper: walk over all document vectors
 * 			2.1 determine if the document occurs in the TREC result file
 * 			2.2 if so, compute the weight vector and emit (qid,rank)(docid,[termid1 weight1 termid2 weight2 ...])
 * 
 * (3) MyPartitioner: ensure that all keys (qid,rank) with the same qid end up in the same reducer; should be sorted by rank in the secondary sort
 * 
 * (4) MyReducer: for each query
 * 			4.1 store the weight vector of docid at rank 1
 * 			4.2 for each subsequent weight vector, compare to all previously kept weight vectors by computing cosine
 * 			4.3 if similarity above threshold, discard document, otherwise emit it (in TREC result file format) and store it in memory for keep lookup
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
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.BytesWritable;
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
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.util.Version;
import org.clueweb.data.TermStatistics;
import org.clueweb.data.VByteDocVector;
import org.clueweb.dictionary.DefaultFrequencySortedDictionary;

import tl.lin.data.pair.PairOfIntString;
import tl.lin.data.pair.PairOfInts;
import tl.lin.data.pair.PairOfStringFloat;
import tl.lin.lucene.AnalyzerUtils;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

public class Filtering extends Configured implements Tool {
	
	private static final Logger LOG = Logger.getLogger(Filtering.class);
	
	
	/*
	 * Partitioner: all keys with the same qid go to the same reducer
	 
	private static class MyPartitioner extends Partitioner<PairOfIntString, FloatWritable> {

		@Override
		public int getPartition(PairOfIntString arg0, FloatWritable arg1,
				int numPartitions) {
			return arg0.getLeftElement()%numPartitions;
		}
	}
	*/
	
	/*
	 * we need to emit [termid1 weight1 termid2 weight2 ...] as value in MyMapper
	 */
	private static class FloatArrayWritable extends ArrayWritable 
	{ 
		public FloatArrayWritable() 
		{ 
			super(FloatWritable.class); 
		} 
		
		public FloatArrayWritable(FloatWritable[] values)
		{
			super(FloatWritable.class, values);
		}
	} 

	/*
	 * Mapper outKey: (qid,result file line), value: term weight array
	 */
	private static class MyMapper extends
			Mapper<Text, BytesWritable, PairOfIntString, FloatArrayWritable> {
		
		private static final VByteDocVector DOC = new VByteDocVector();
		private DefaultFrequencySortedDictionary dictionary;
		private TermStatistics stats;

		//complex key: (qid,result file line)
		private static final PairOfIntString keyOut = new PairOfIntString();
		private static final FloatArrayWritable valueOut = new FloatArrayWritable();
		
		//key: docid, value: a set of lines in the TREC result file containing that docid (he same docid can occur in several queries, thus Set as value)
		private static final HashMap<String,HashSet<String>> docidResults = Maps.newHashMap();

		private double numDocs;
		
		@Override
		public void setup(Context context) throws IOException {
			
			FileSystem fs = FileSystem.get(context.getConfiguration());
			String path = context.getConfiguration().get(DICTIONARY_OPTION);
			dictionary = new DefaultFrequencySortedDictionary(path, fs);
			stats = new TermStatistics(new Path(path), fs);
			numDocs = stats.getCollectionSize();

			FSDataInputStream fsin = fs.open(new Path(context
					.getConfiguration().get(TREC_RESULT_FILE)));
			BufferedReader br = new BufferedReader(new InputStreamReader(fsin));
			String line;
			while ((line = br.readLine()) != null) {
				
				String tokens[] = line.split("\\s+");
				String did = tokens[2];
				HashSet<String> set = null;
				if(docidResults.containsKey(did))
					set = docidResults.get(did);
				else
				{
					set = Sets.newHashSet();
					docidResults.put(did, set);
				}
				set.add(line);
			}
			fsin.close();
			br.close();
		}

		@Override
		public void map(Text key, BytesWritable bytes, Context context)
				throws IOException, InterruptedException {

			VByteDocVector.fromBytesWritable(bytes, DOC);
			
			//is the document of interest to us?
			if(!docidResults.containsKey(key.toString()))
				return;

			//tfMap of the document
			HashMap<Integer, Integer> tfMap = Maps.newHashMap();
			for (int termid : DOC.getTermIds()) {
				int tf = 1;
				if (tfMap.containsKey(termid))
					tf += tfMap.get(termid);
				tfMap.put(termid, tf);
			}
			
			FloatWritable[] fw = new FloatWritable[DOC.getLength()*2];
			
			int index=0;
			for(int termid: tfMap.keySet())
			{
				double weight = (double)tfMap.get(termid) * Math.log(  numDocs / (double)(stats.getDf(termid)) );
				fw[index++]=new FloatWritable(termid);
				fw[index++]=new FloatWritable((float)weight);
			}
			
			for(String line : docidResults.get(key.toString()))
			{
				String tokens[] = line.split("\\s+");
				int qid = Integer.parseInt(tokens[0]);
				
				keyOut.set(qid, line);
				valueOut.set(fw);
				context.write(keyOut, valueOut);
			}
		}
	}


	private static class MyReducer extends
			Reducer<PairOfIntString, FloatArrayWritable, NullWritable, Text> {

		private float threshold;
		private int topk;
		private int currentQuery;
		private static final NullWritable nullKey = NullWritable.get();
		private static final Text valueOut = new Text();

		public void setup(Context context) throws IOException {
			try {
				threshold = Float.parseFloat(context.getConfiguration().get(SIM_THRESHOLD));
				topk = Integer.parseInt(context.getConfiguration().get(TOPK));
			} catch (NumberFormatException e) {
				LOG.info("sim threshold should parse into a float, instead is "
						+ context.getConfiguration().get(SIM_THRESHOLD));
				threshold = 0.9f;
				LOG.info("Setting sim. threshold to "+threshold);
			}
			currentQuery = -1;
			
			//sanity check
			if(threshold<0||threshold>1)
			{
				LOG.info("Error: threshold should always be between 0 and 1. Setting default to 0.9");
				threshold = 0.9f;
			}
			
			if(topk<0)
			{
				LOG.info("Error: topk must be positive. Trying default setting of 1000");
				topk=1000;
			}
		}

		@Override
		public void reduce(PairOfIntString key, Iterable<FloatArrayWritable> values,
				Context context) throws IOException, InterruptedException {

			//we hit a new query, process what is currently in the priority queue
			if(key.getLeftElement()!=currentQuery)
			{
				if(currentQuery>0)
				{
					//convert minimum heap into list in ascending order
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
			
			int qid = key.getLeftElement();
			String line = key.getRightElement();
			
			//should be a single element
			for(FloatArrayWritable faw : values)
			{
				faw.
			}
		}
		
		
		//in the cleanup, we still have to deal with the final query
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

		LOG.info("Tool name: " + Filtering.class.getSimpleName());
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

		Job job = new Job(conf, Filtering.class.getSimpleName() + ":"
				+ vbdocvector);
		job.setJarByClass(Filtering.class);

		FileInputFormat.setInputPaths(job, vbdocvector);
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
		LOG.info("Running " + Filtering.class.getCanonicalName()
				+ " with args " + Arrays.toString(args));
		ToolRunner.run(new Filtering(), args);
	}
}
