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
 * @author Claudia Hauff
 * 
 * Takes as input a file with docids (one per line) and outputs their content into the output folder.
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
import java.util.Map;
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
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.util.Version;
import org.clueweb.clueweb12.ClueWeb12WarcRecord;
import org.clueweb.clueweb12.mapreduce.ClueWeb12InputFormat;
import org.clueweb.data.TermStatistics;
import org.clueweb.dictionary.DefaultFrequencySortedDictionary;
import org.clueweb.dictionary.PorterAnalyzer;
import org.clueweb.util.AnalyzerFactory;
import org.jsoup.Jsoup;
import org.mortbay.log.Log;

import tl.lin.data.pair.PairOfIntLong;
import tl.lin.lucene.AnalyzerUtils;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

public class DocumentExtractor extends Configured implements Tool {
	private static final Logger LOG = Logger
			.getLogger(DocumentExtractor.class);

	private static boolean keepHTML;
	private static final HashMap<String,String> docidMap = Maps.newHashMap();
	private static final String EMPTY = "";

	private static class MyMapper extends
			Mapper<LongWritable, ClueWeb12WarcRecord, NullWritable, NullWritable> {
		
		@Override
		public void setup(Context context) throws IOException {

			FileSystem fs = FileSystem.get(context.getConfiguration());	
			FSDataInputStream fsin = fs.open(new Path(context
					.getConfiguration().get(DOCIDS_FILE)));
			
			keepHTML = context.getConfiguration().getBoolean(KEEP_HTML, true);
			LOG.info("keephtml is set to "+keepHTML);
			
			BufferedReader br = new BufferedReader(new InputStreamReader(fsin));
			String line;
			while ((line = br.readLine()) != null) {
				docidMap.put(line,EMPTY);
			}
			fsin.close();
			br.close();
			
			LOG.info("Number of docids read from "+context.getConfiguration().get(DOCIDS_FILE)+": "+docidMap.size());
		}


		@Override
		public void map(LongWritable key, ClueWeb12WarcRecord doc,
				Context context) throws IOException, InterruptedException {

			String docid = doc.getHeaderMetadataItem("WARC-TREC-ID");
			if (docid != null && docidMap.containsKey(docid)) {
				try {
					String content = doc.getContent();

					if(!keepHTML) {
						content = Jsoup.parse(content).text();
					}
					docidMap.put(docid, content);

				} catch (Exception e) {
					// If Jsoup throws any exceptions, catch and move on.
					LOG.info("Error caught processing " + docid);
				}
			}
		}
		
		@Override
		public void cleanup(Context context) throws IOException {

			FileSystem fs = FileSystem.get(context.getConfiguration());	
			String outputFolder = context.getConfiguration().get(OUTPUT_OPTION);
			if(!outputFolder.endsWith("/"))
				outputFolder += "/";
			
			for(String docid : docidMap.keySet())
			{
				if(docidMap.get(docid).equals(EMPTY)) {
					continue;
				}
				
				Path p = new Path(outputFolder+docid);
				FSDataOutputStream fsout = fs.create(p);
				BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fsout));
				bw.write(docidMap.get(docid));
				fsout.close();
				bw.close();
				
				LOG.info("Written document content to "+p.toString());
			}
		}
	}


	public static final String INPUT_OPTION = "input";
	public static final String OUTPUT_OPTION = "output";
	public static final String DOCIDS_FILE = "docidsfile";
	public static final String KEEP_HTML = "keephtml";

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
		options.addOption(OptionBuilder.withArgName("path").hasArg()
				.withDescription("docids file path").create(DOCIDS_FILE));
		options.addOption(OptionBuilder.withArgName("true|false").hasArg()
				.withDescription("keep HTML").create(KEEP_HTML));

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

		if (!cmdline.hasOption(INPUT_OPTION)
				|| !cmdline.hasOption(OUTPUT_OPTION)
				|| !cmdline.hasOption(DOCIDS_FILE)
				|| !cmdline.hasOption(KEEP_HTML)) {
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp(this.getClass().getName(), options);
			ToolRunner.printGenericCommandUsage(System.out);
			return -1;
		}

		String input = cmdline.getOptionValue(INPUT_OPTION);
		String output = cmdline.getOptionValue(OUTPUT_OPTION);
		String docidsfile = cmdline.getOptionValue(DOCIDS_FILE);
		boolean keephtml = (cmdline.getOptionValue(KEEP_HTML).equals("true"))?true:false;

		LOG.info("Tool name: " + DocumentExtractor.class.getSimpleName());
		LOG.info(" - input: " + input);
		LOG.info(" - output: " + output);
		LOG.info(" - docidsfile: "+docidsfile);
		Log.info(" - keephtml: "+keephtml);

		Configuration conf = getConf();
		conf.set(DOCIDS_FILE, docidsfile);
		conf.setBoolean(KEEP_HTML,keephtml);
		conf.set(OUTPUT_OPTION, output);
		
		Job job = new Job(getConf(),
				DocumentExtractor.class.getSimpleName() + ":" + input);
		job.setJarByClass(DocumentExtractor.class);

		FileInputFormat.setInputPaths(job, input);
		job.setInputFormatClass(ClueWeb12InputFormat.class);
		job.setMapOutputKeyClass(NullWritable.class);
		job.setMapOutputValueClass(NullWritable.class);

		job.setMapperClass(MyMapper.class);

		FileSystem.get(getConf()).delete(new Path(output), true);

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
		LOG.info("Running " + DocumentExtractor.class.getCanonicalName()
				+ " with args " + Arrays.toString(args));
		ToolRunner.run(new DocumentExtractor(), args);
	}
}
