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

import java.io.IOException;
import java.util.Arrays;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import tl.lin.data.pair.PairOfIntLong;

public class MergeTermStatistics extends Configured implements Tool {
	private static final Logger LOG = Logger
			.getLogger(MergeTermStatistics.class);

	private static final String HADOOP_DF_MIN_OPTION = "df.min";
	private static final String HADOOP_DF_MAX_OPTION = "df.max";

	private static final int MIN_DF_DEFAULT = 100; // Throw away terms with df
													// less than this.

	private static class MyCombiner extends
			Reducer<Text, PairOfIntLong, Text, PairOfIntLong> {
		private static final PairOfIntLong output = new PairOfIntLong();

		@Override
		public void reduce(Text key, Iterable<PairOfIntLong> values,
				Context context) throws IOException, InterruptedException {
			int df = 0;
			long cf = 0;
			for (PairOfIntLong pair : values) {
				df += pair.getLeftElement();
				cf += pair.getRightElement();
			}

			output.set(df, cf);
			context.write(key, output);
		}
	}

	private static class MyReducer extends
			Reducer<Text, PairOfIntLong, Text, PairOfIntLong> {
		private static final PairOfIntLong output = new PairOfIntLong();
		private int dfMin, dfMax;

		@Override
		public void setup(
				Reducer<Text, PairOfIntLong, Text, PairOfIntLong>.Context context) {
			dfMin = context.getConfiguration().getInt(HADOOP_DF_MIN_OPTION,
					MIN_DF_DEFAULT);
			dfMax = context.getConfiguration().getInt(HADOOP_DF_MAX_OPTION,
					Integer.MAX_VALUE);
			LOG.info("dfMin = " + dfMin);
		}

		@Override
		public void reduce(Text key, Iterable<PairOfIntLong> values,
				Context context) throws IOException, InterruptedException {
			int df = 0;
			long cf = 0;
			for (PairOfIntLong pair : values) {
				df += pair.getLeftElement();
				cf += pair.getRightElement();
			}
			if (df < dfMin || df > dfMax) {
				return;
			}
			output.set(df, cf);
			context.write(key, output);
		}
	}

	public static final String INPUT_OPTION = "input";
	public static final String OUTPUT_OPTION = "output";
	public static final String DF_MIN_OPTION = "dfMin";

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
		options.addOption(OptionBuilder.withArgName("num").hasArg()
				.withDescription("minimum df").create(DF_MIN_OPTION));

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
				|| !cmdline.hasOption(OUTPUT_OPTION)) {
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp(this.getClass().getName(), options);
			ToolRunner.printGenericCommandUsage(System.out);
			return -1;
		}

		String input = cmdline.getOptionValue(INPUT_OPTION);
		String output = cmdline.getOptionValue(OUTPUT_OPTION);

		LOG.info("Tool name: " + MergeTermStatistics.class.getSimpleName());
		LOG.info(" - input: " + input);
		LOG.info(" - output: " + output);

		Job job = new Job(getConf(), MergeTermStatistics.class.getSimpleName()
				+ ":" + input);
		job.setJarByClass(MergeTermStatistics.class);

		job.setNumReduceTasks(1);

		if (cmdline.hasOption(DF_MIN_OPTION)) {
			int dfMin = Integer.parseInt(cmdline.getOptionValue(DF_MIN_OPTION));
			LOG.info(" - dfMin: " + dfMin);
			job.getConfiguration().setInt(HADOOP_DF_MIN_OPTION, dfMin);
		}

		FileInputFormat.setInputPaths(job, input);
		FileOutputFormat.setOutputPath(job, new Path(output));

		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(PairOfIntLong.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(PairOfIntLong.class);

		job.setCombinerClass(MyCombiner.class);
		job.setReducerClass(MyReducer.class);

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
		LOG.info("Running " + MergeTermStatistics.class.getCanonicalName()
				+ " with args " + Arrays.toString(args));
		ToolRunner.run(new MergeTermStatistics(), args);
	}
}
