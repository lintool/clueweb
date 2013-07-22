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
 * Implementation of cosine-based near-duplicate filtering: given a TREC result file as input, each document in the ranking
 * is compared with all documents ranked before it - if it has a cosine similarity > X to one of them, it is discarded as duplicate.
 * Note: similarity based on TF.IDF weighted cosine
 * 
 * Approach:
 *  
 * (1) MyMapper: walk over all document vectors
 * 			2.1 determine if the document occurs in the TREC result file
 * 			2.2 if yes, compute the weight vector and emit (qid,line)([termid1 weight1 termid2 weight2 ...])
 * 				the complex key consists of query ID and the entire line in the TREC result file
 * 				the value is a FloatArrayWritable containing term ids and term weights of the document
 * 
 * (2) MyPartitioner: make sure that all key/values of the same query end up in the same Reducer
 * 
 * (3) MyReducer: for each query
 * 			4.1 accumulate all the documents term weight arrays that were emitted in the map() stage
 * 			4.2 the top ranked document is not a duplicate, all lower ranked documents are compared to higher ranked ones;
 * 				if the similarity is above the threshold, the document is considered a duplicate
 * 			4.3 all TREC result file lines are emitted whose documents were not identified as duplicates
 * 
 * TODO: reducer code duplicated in reduce() and cleanup(); needs fixing
 */
package org.clueweb.clueweb12.app;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;

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
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
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

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

public class Filtering extends Configured implements Tool {
	
	private static final Logger LOG = Logger.getLogger(Filtering.class);
	
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
	 * Partitioner: all keys with the same qid go to the same reducer
	 */
	private static class MyPartitioner extends Partitioner<PairOfIntString, FloatArrayWritable> {

		@Override
		public int getPartition(PairOfIntString arg0, FloatArrayWritable arg1,
				int numPartitions) {
			return arg0.getLeftElement()%numPartitions;
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
			
			//create an array of [termid1 weight1 termid2 weight2 .... ] for the document
			FloatWritable[] fw = new FloatWritable[tfMap.size()*2];
			
			int index=0;
			for(int termid: tfMap.keySet())
			{
				//TF.IDF weights
				double weight = (double)tfMap.get(termid) * Math.log(  numDocs / (double)(stats.getDf(termid)) );
				fw[index++]=new FloatWritable(termid);
				fw[index++]=new FloatWritable((float)weight);
			}
			
			for(String line : docidResults.get(key.toString()))
			{
				int qid = Integer.parseInt(line.split("\\s+")[0]);
				
				keyOut.set(qid, line);
				valueOut.set(fw);
				context.write(keyOut, valueOut);
			}
			
		}
	}


	private static class MyReducer extends
			Reducer<PairOfIntString, FloatArrayWritable, NullWritable, Text> {

		private int topk;
		private float threshold;
		private int currentQuery;
		private static final NullWritable nullKey = NullWritable.get();
		private static final Text valueOut = new Text();
		private static final HashMap<Integer, HashMap<Integer,Float>> termWeights = Maps.newHashMap();
		private static final HashMap<Integer, String> resultFileLines = Maps.newHashMap();
		
		
		private BufferedWriter logWriter;
		

		public void setup(Context context) throws IOException {
			try {
				threshold = Float.parseFloat(context.getConfiguration().get(SIM_THRESHOLD));
			} catch (NumberFormatException e) {
				LOG.info("sim threshold should parse into a float, instead is "
						+ context.getConfiguration().get(SIM_THRESHOLD));
				threshold = 0.9f;
				LOG.info("Setting sim. threshold to "+threshold);
			}
			
			try {
				topk = Integer.parseInt(context.getConfiguration().get(TOPK));
			} catch (NumberFormatException e) {
				LOG.info("topk should parse into an int, instead is "
						+ context.getConfiguration().get(TOPK));
				topk = 1000;
				LOG.info("Setting topk to "+topk);
			}
			
			currentQuery = -1;
			
			//sanity check
			if(threshold<0||threshold>1)
			{
				LOG.info("Error: threshold should always be between 0 and 1. Setting default to 0.9");
				threshold = 0.9f;
			}
			
			int r = (int)(Math.random()*10000);

			FileSystem fs = FileSystem.get(context.getConfiguration());
			FSDataOutputStream fsout = fs.create(new Path("/user/claudiah/sim_"+r));
			logWriter = new BufferedWriter(new OutputStreamWriter(fsout));
	
		}
		
		private double squaredSum(HashMap<Integer,Float> map)
		{
			double ssum=0.0;
			for(int key : map.keySet())
				ssum+=map.get(key)*map.get(key);
			return ssum;
		}
		
		private double computeCosineSim(HashMap<Integer,Float> map1, HashMap<Integer,Float> map2)
		{
			double denominator1=Math.sqrt(squaredSum(map1));
			double denominator2=Math.sqrt(squaredSum(map2));
			
			double denominator = denominator1*denominator2;
			
			double numerator = 0.0;
			for(int key1 : map1.keySet())
			{
				if(map2.containsKey(key1))
					numerator += map1.get(key1)*map2.get(key1);
			}
			return (numerator/denominator);
		}

		
		@Override
		public void reduce(PairOfIntString key, Iterable<FloatArrayWritable> values,
				Context context) throws IOException, InterruptedException {
	
			logWriter.write("in reduce() at query "+key.getLeftElement()+"\n");
			logWriter.write("termWeights.size(): "+termWeights.size()+"\n");
			
			
			//extract the rank
			String tokens[] = key.getRightElement().split("\\s+");
			int rank = Integer.parseInt(tokens[3]);
			
			logWriter.write("rank: "+rank+"\n");
			
			//we hit a new query and need to process the old query first
			if(termWeights.size()>0 && currentQuery!=key.getLeftElement() && currentQuery>0)
			{
				logWriter.write("new query, evaluating!\n");
				for(int r=2; r<=topk; r++)
				{
					if(termWeights.containsKey(r)==false)
						continue;

					for(int s=(r-1); s>=1; s--)
					{
						if(termWeights.containsKey(s)==false)
							continue;
						
						double sim = computeCosineSim(termWeights.get(r), termWeights.get(s));
						
						logWriter.write("COMPARING RANK "+s+" WITH RANK "+r+" sim="+sim);
						logWriter.newLine();
						
						if(sim>=threshold)
						{
							termWeights.remove(r);
							break;
						}
					}
				}
				
				//whatever is left is not a duplicate ...
				for(int r=1; r<=topk; r++)
				{
					if(termWeights.containsKey(r))
					{
						valueOut.set(resultFileLines.get(r));
						context.write(nullKey, valueOut);
					}
				}
				
				termWeights.clear();
				resultFileLines.clear();
			}
			
			currentQuery = key.getLeftElement();
			
			resultFileLines.put(rank, key.getRightElement());
			
			HashMap<Integer, Float> weights = Maps.newHashMap();
			Writable array[] = values.iterator().next().get();
			for(int i=0; i<array.length; i+=2)
			{
				int termid = (int)((FloatWritable)array[i]).get();
				float weight = ((FloatWritable)array[i+1]).get();
				weights.put(termid, weight);
			}
			termWeights.put(rank, weights);
			
		}
		
		
		//in the cleanup, we still have to deal with the final query
		public void cleanup(Context context) throws IOException, InterruptedException {

			logWriter.write("in cleanup\n");
			logWriter.write("termWeights.size(): "+termWeights.size()+"\n");

			logWriter.write("new query, evaluating!\n");
			for(int r=2; r<=topk; r++)
			{
				if(termWeights.containsKey(r)==false)
					continue;

				for(int s=(r-1); s>=1; s--)
				{
					if(termWeights.containsKey(s)==false)
						continue;
					
					double sim = computeCosineSim(termWeights.get(r), termWeights.get(s));
					
					logWriter.write("COMPARING RANK "+s+" WITH RANK "+r+" sim="+sim);
					logWriter.newLine();
					
					if(sim>=threshold)
					{
						termWeights.remove(r);
						break;
					}
				}
			}
			
			//whatever is left is not a duplicate ...
			for(int r=1; r<=topk; r++)
			{
				if(termWeights.containsKey(r))
				{
					valueOut.set(resultFileLines.get(r));
					context.write(nullKey, valueOut);
				}
			}
			
			
			logWriter.close();
		}
	}

	public static final String VBDOCVECTOR_OPTION = "vbdocvector";
	public static final String OUTPUT_OPTION = "output";
	public static final String DICTIONARY_OPTION = "dictionary";
	public static final String QUERIES_OPTION = "queries";
	public static final String TOPK = "topk";
	public static final String SIM_THRESHOLD = "threshold";
	public static final String TREC_RESULT_FILE = "trecinputfile";

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
				.withDescription("input path")
				.create(TREC_RESULT_FILE));
		options.addOption(OptionBuilder.withArgName("path").hasArg()
				.withDescription("output path").create(OUTPUT_OPTION));
		options.addOption(OptionBuilder.withArgName("path").hasArg()
				.withDescription("dictionary").create(DICTIONARY_OPTION));
		options.addOption(OptionBuilder.withArgName("int").hasArg()
				.withDescription("topk").create(TOPK));
		options.addOption(OptionBuilder.withArgName("double").hasArg()
				.withDescription("threshold").create(SIM_THRESHOLD));

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
				|| !cmdline.hasOption(TREC_RESULT_FILE)
				|| !cmdline.hasOption(SIM_THRESHOLD) 
				|| !cmdline.hasOption(TOPK)) {
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp(this.getClass().getName(), options);
			ToolRunner.printGenericCommandUsage(System.out);
			return -1;
		}

		String vbdocvector = cmdline.getOptionValue(VBDOCVECTOR_OPTION);
		String trecinput = cmdline.getOptionValue(TREC_RESULT_FILE);
		String output = cmdline.getOptionValue(OUTPUT_OPTION);
		String dictionary = cmdline.getOptionValue(DICTIONARY_OPTION);
		String threshold = cmdline.getOptionValue(SIM_THRESHOLD);
		String topk = cmdline.getOptionValue(TOPK);

		LOG.info("Tool name: " + Filtering.class.getSimpleName());
		LOG.info(" - vbdocvector: " + vbdocvector);
		LOG.info(" - trecinputfile: "+ trecinput);
		LOG.info(" - output: " + output);
		LOG.info(" - dictionary: " + dictionary);
		LOG.info(" - threshold: " + SIM_THRESHOLD);
		LOG.info(" - topk: " + topk);

		Configuration conf = getConf();
		conf.set(DICTIONARY_OPTION, dictionary);
		conf.set(SIM_THRESHOLD, threshold);
		conf.set(TREC_RESULT_FILE, trecinput);
		conf.set(TOPK, topk);
		
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
		job.setMapOutputValueClass(FloatArrayWritable.class);
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
