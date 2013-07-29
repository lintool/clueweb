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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.FieldInfo.IndexOptions;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;
import org.apache.tools.bzip2.CBZip2InputStream;
import org.clueweb.data.WarcTrecIdMapping;

public class BuildWarcTrecIdMapping {
	private static final Logger LOG = Logger
			.getLogger(BuildWarcTrecIdMapping.class);

	public static final Analyzer ANALYZER = new StandardAnalyzer(
			Version.LUCENE_43);

	static final FieldType FIELD_OPTIONS = new FieldType();

	static {
		FIELD_OPTIONS.setIndexed(true);
		FIELD_OPTIONS.setIndexOptions(IndexOptions.DOCS_ONLY);
		FIELD_OPTIONS.setStored(true);
		FIELD_OPTIONS.setTokenized(false);
	}

	private static final int DEFAULT_NUM_THREADS = 4;

	private static final String INPUT_OPTION = "input";
	private static final String INDEX_OPTION = "index";
	private static final String MAX_OPTION = "max";
	private static final String OPTIMIZE_OPTION = "optimize";
	private static final String THREADS_OPTION = "threads";

	@SuppressWarnings("static-access")
	public static void main(String[] args) throws Exception {
		Options options = new Options();
		options.addOption(OptionBuilder.withArgName("path").hasArg()
				.withDescription("bz2 Wikipedia XML dump file")
				.create(INPUT_OPTION));
		options.addOption(OptionBuilder.withArgName("dir").hasArg()
				.withDescription("index location").create(INDEX_OPTION));
		options.addOption(OptionBuilder.withArgName("num").hasArg()
				.withDescription("maximum number of documents to index")
				.create(MAX_OPTION));
		options.addOption(OptionBuilder.withArgName("num").hasArg()
				.withDescription("number of indexing threads")
				.create(THREADS_OPTION));

		options.addOption(new Option(OPTIMIZE_OPTION,
				"merge indexes into a single segment"));

		CommandLine cmdline = null;
		CommandLineParser parser = new GnuParser();
		try {
			cmdline = parser.parse(options, args);
		} catch (ParseException exp) {
			System.err.println("Error parsing command line: "
					+ exp.getMessage());
			System.exit(-1);
		}

		if (!cmdline.hasOption(INPUT_OPTION)
				|| !cmdline.hasOption(INDEX_OPTION)) {
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp(
					BuildWarcTrecIdMapping.class.getCanonicalName(), options);
			System.exit(-1);
		}

		String indexPath = cmdline.getOptionValue(INDEX_OPTION);
		int maxdocs = cmdline.hasOption(MAX_OPTION) ? Integer.parseInt(cmdline
				.getOptionValue(MAX_OPTION)) : Integer.MAX_VALUE;
		int threads = cmdline.hasOption(THREADS_OPTION) ? Integer
				.parseInt(cmdline.getOptionValue(THREADS_OPTION))
				: DEFAULT_NUM_THREADS;

		long startTime = System.currentTimeMillis();

		String path = cmdline.getOptionValue(INPUT_OPTION);
		PrintStream out = new PrintStream(System.out, true, "UTF-8");

		Directory dir = FSDirectory.open(new File(indexPath));
		IndexWriterConfig config = new IndexWriterConfig(Version.LUCENE_43,
				ANALYZER);
		config.setOpenMode(OpenMode.CREATE);

		IndexWriter writer = new IndexWriter(dir, config);
		LOG.info("Creating index at " + indexPath);
		LOG.info("Indexing with " + threads + " threads");

		FileInputStream fis = null;
		BufferedReader br = null;

		try {
			fis = new FileInputStream(new File(path));
			byte[] ignoreBytes = new byte[2];
			fis.read(ignoreBytes); // "B", "Z" bytes from commandline tools
			br = new BufferedReader(new InputStreamReader(
					new CBZip2InputStream(fis), "UTF8"));

			ExecutorService executor = Executors.newFixedThreadPool(threads);
			int cnt = 0;
			String s;
			while ((s = br.readLine()) != null) {
				Runnable worker = new AddDocumentRunnable(writer, s);
				executor.execute(worker);

				cnt++;
				if (cnt % 1000000 == 0) {
					LOG.info(cnt + " articles added");
				}
				if (cnt >= maxdocs) {
					break;
				}
			}

			executor.shutdown();
			// Wait until all threads are finish
			while (!executor.isTerminated()) {
			}

			LOG.info("Total of " + cnt + " articles indexed.");

			if (cmdline.hasOption(OPTIMIZE_OPTION)) {
				LOG.info("Merging segments...");
				writer.forceMerge(1);
				LOG.info("Done!");
			}

			LOG.info("Total elapsed time: "
					+ (System.currentTimeMillis() - startTime) + "ms");
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			writer.close();
			dir.close();
			out.close();
			br.close();
			fis.close();
		}
	}

	private static class AddDocumentRunnable implements Runnable {
		private final IndexWriter writer;
		private final String s;

		AddDocumentRunnable(IndexWriter writer, String s) {
			this.writer = writer;
			this.s = s.split(",")[0];
		}

		@Override
		public void run() {
			Document doc = new Document();
			doc.add(new Field(WarcTrecIdMapping.IndexField.WARC_TREC_ID.name,
					s, FIELD_OPTIONS));

			try {
				writer.addDocument(doc);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
}
