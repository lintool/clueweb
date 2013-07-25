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
 * Approach:
 * 
 * MyMapper: 
 * 	in setup() read the docids that make an appearance in the TREC result file (one of the inputs)
 * 	in map() walk over all spam score files and determine which docids from the step above are spam
 * 	in cleanup() read the TREC result file one more time and emit all lines that have non-spam documents
 * 
 * MyReducer:
 * 	nothing to do, passing through the values
 * 
 */
package org.clueweb.clueweb12.app;

import java.io.BufferedReader;

import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.HashMap;
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
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import tl.lin.data.pair.PairOfInts;

import com.google.common.collect.Maps;

public class SpamScoreFiltering extends Configured implements Tool {

	private static final Logger LOG = Logger
			.getLogger(SpamScoreFiltering.class);
	private static int autoIncrement = 0;

	private static class MyMapper extends
			Mapper<LongWritable, Text, PairOfInts, Text> {

		private static final PairOfInts keyOut = new PairOfInts();
		private static final Text valueOut = new Text();

		// key: docid, value: spam indicator
		HashMap<String, Integer> docids = Maps.newHashMap();
		private int spamThreshold;

		@Override
		public void setup(Context context) throws IOException {

			FileSystem fs = FileSystem.get(context.getConfiguration());
			spamThreshold = context.getConfiguration().getInt(SPAM_THRESHOLD,
					70);
			LOG.info("spam threshold set to " + spamThreshold);

			FSDataInputStream fsin = fs.open(new Path(context
					.getConfiguration().get(TREC_RESULT_FILE)));
			BufferedReader br = new BufferedReader(new InputStreamReader(fsin));
			String line;
			while ((line = br.readLine()) != null) {
				String tokens[] = line.split("\\s+");
				docids.put(tokens[2], 0);
			}
			fsin.close();
			br.close();
		}

		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {

			String tokens[] = value.toString().split("\\s+");
			

			if (!docids.containsKey(tokens[1])) {
				return;
			}

			// declare as spam (the higher the score the less spammy it is)
			if (Integer.parseInt(tokens[0]) < spamThreshold)
			{
				docids.put(tokens[1], -1);//spam
			}
			else
			{
				docids.put(tokens[1], 1);//no spam
			}
		}

		@Override
		public void cleanup(Context context) throws IOException,
				InterruptedException {

			FileSystem fs = FileSystem.get(context.getConfiguration());

			FSDataInputStream fsin = fs.open(new Path(context
					.getConfiguration().get(TREC_RESULT_FILE)));
			BufferedReader br = new BufferedReader(new InputStreamReader(fsin));
			String line;
			while ((line = br.readLine()) != null) {
				String tokens[] = line.split("\\s+");
				if (docids.get(tokens[2]) == 1) {
					keyOut.set(Integer.parseInt(tokens[0]),Integer.parseInt(tokens[3]));
					valueOut.set(line);
					context.write(keyOut, valueOut);
				}
			}
			fsin.close();
			br.close();
		}
	}

	private static class MyReducer extends
			Reducer<PairOfInts, Text, NullWritable, Text> {

		private static final NullWritable nullKey = NullWritable.get();

		@Override
		public void reduce(PairOfInts key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {

			context.write(nullKey, values.iterator().next());
		}
	}

	public static final String OUTPUT_OPTION = "output";
	public static final String TREC_RESULT_FILE = "trecinputfile";
	public static final String SPAM_THRESHOLD = "spamThreshold";
	public static final String SPAM_SCORES_FOLDER = "spamScoreFolder";

	/**
	 * Runs this tool.
	 */
	@SuppressWarnings({ "static-access", "deprecation" })
	public int run(String[] args) throws Exception {
		Options options = new Options();

		options.addOption(OptionBuilder.withArgName("path").hasArg()
				.withDescription("input path").create(TREC_RESULT_FILE));
		options.addOption(OptionBuilder.withArgName("path").hasArg()
				.withDescription("output path").create(OUTPUT_OPTION));
		options.addOption(OptionBuilder.withArgName("int [0-99]").hasArg()
				.withDescription("spam threshold").create(SPAM_THRESHOLD));
		options.addOption(OptionBuilder.withArgName("path").hasArg()
				.withDescription("spam scores folder")
				.create(SPAM_SCORES_FOLDER));

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

		if (!cmdline.hasOption(OUTPUT_OPTION)
				|| !cmdline.hasOption(TREC_RESULT_FILE)
				|| !cmdline.hasOption(SPAM_THRESHOLD)
				|| !cmdline.hasOption(SPAM_SCORES_FOLDER)) {
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp(this.getClass().getName(), options);
			ToolRunner.printGenericCommandUsage(System.out);
			return -1;
		}

		String trecinput = cmdline.getOptionValue(TREC_RESULT_FILE);
		String output = cmdline.getOptionValue(OUTPUT_OPTION);
		String spamThreshold = cmdline.getOptionValue(SPAM_THRESHOLD);
		String spamScoreFolder = cmdline.getOptionValue(SPAM_SCORES_FOLDER);

		LOG.info("Tool name: " + SpamScoreFiltering.class.getSimpleName());
		LOG.info(" - trecinputfile: " + trecinput);
		LOG.info(" - output: " + output);
		LOG.info(" - spam threshold: " + spamThreshold);
		LOG.info(" - spam scores folder: " + spamScoreFolder);

		Configuration conf = getConf();
		conf.setInt(SPAM_THRESHOLD, Integer.parseInt(spamThreshold));
		conf.set(TREC_RESULT_FILE, trecinput);

		FileSystem fs = FileSystem.get(conf);
		if (fs.exists(new Path(output)))
			fs.delete(new Path(output));

		Job job = new Job(conf, SpamScoreFiltering.class.getSimpleName());
		job.setJarByClass(SpamScoreFiltering.class);
		job.setNumReduceTasks(1);

		FileInputFormat.setInputPaths(job, spamScoreFolder);
		FileOutputFormat.setOutputPath(job, new Path(output));

		job.setInputFormatClass(TextInputFormat.class);

		job.setMapOutputKeyClass(PairOfInts.class);
		job.setMapOutputValueClass(Text.class);
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
		LOG.info("Running " + SpamScoreFiltering.class.getCanonicalName()
				+ " with args " + Arrays.toString(args));
		ToolRunner.run(new SpamScoreFiltering(), args);
	}
}
