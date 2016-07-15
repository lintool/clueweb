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
 * Either:
 *  - Takes as input a file with docids (one per line) and writes their content to file.
 *  - If docidsfile is "-1", all encountered content is written to files (in batches of 10,000 docs to file)
 *  
 * MyMapper:
 * 	2.1 the docids are read from the file in setup()
 * 	2.2 all *warc.gz files are read in map() and if the right docid is hit, the content is stored
 * 	2.3 the cleanup() method writes the content of all found docids to file
 * 
 * Reducer: no reducer necessary, all work is done in MyMapper
 */

package org.clueweb.clueweb12.app;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
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
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.clueweb.clueweb12.ClueWeb12WarcRecord;
import org.clueweb.clueweb12.mapreduce.ClueWeb12InputFormat;
import org.clueweb.util.HTMLParserFactory;
import com.google.common.collect.Maps;

public class DocumentExtractor extends Configured implements Tool {
  private static final Logger LOG = Logger.getLogger(DocumentExtractor.class);

  private static enum Records {
    DOCUMENTS_FOUND, HTML_PARSER_EXCEPTIONS
  };

  private static boolean keepHTML;
  private static String htmlParser;
  private static boolean writeAll;
  private static final String EMPTY = "";
  private static int fileNumber = 1;

  private static class MyMapper extends
      Mapper<LongWritable, ClueWeb12WarcRecord, NullWritable, NullWritable> {

    private HashMap<String, String> docidMap;
    @Override
    public void setup(Context context) throws IOException {
      
      docidMap = Maps.newHashMap();

      FileSystem fs = FileSystem.get(context.getConfiguration());
      String docidsFile = context.getConfiguration().get(DOCIDS_FILE);
      
      writeAll = (docidsFile.equals("-1")) ? true : false;
      keepHTML = context.getConfiguration().getBoolean(KEEP_HTML, true);
      htmlParser = context.getConfiguration().get(HTML_PARSER);

      if(writeAll==false) {
        FSDataInputStream fsin = fs.open(new Path(docidsFile));
        BufferedReader br = new BufferedReader(new InputStreamReader(fsin));
        String line;
        while ((line = br.readLine()) != null) {
          if (line.length() > 5) {
            docidMap.put(line.toLowerCase(), EMPTY);
          }
        }
        fsin.close();
        br.close();
        LOG.info("Number of docids read from " + docidsFile + ": "
            + docidMap.size());
      }
      else {
        LOG.info("Writing all documents encountered to file");
      }
    }

    @Override
    public void map(LongWritable key, ClueWeb12WarcRecord doc, Context context) throws IOException,
        InterruptedException {

      try {
        //from time to time write the hashmap content to file
        if(docidMap.size()>10000) {
          batchWrite(context);
          docidMap.clear();
        }
    
        String docid = doc.getHeaderMetadataItem("WARC-TREC-ID").toLowerCase();
        if (docid != null && (docidMap.containsKey(docid) || writeAll==true) ) {

          if (keepHTML==false) {
            docidMap.put(docid, HTMLParserFactory.parse(htmlParser, doc.getContent()));
          } else {
            docidMap.put(docid, doc.getContent());
          }
          context.getCounter(Records.DOCUMENTS_FOUND).increment(1);
        } 
      }
      catch (Exception e) {
        // If Jsoup throws any exceptions, catch and move on.
        LOG.info("Error caught when processing a document");
        context.getCounter(Records.HTML_PARSER_EXCEPTIONS).increment(1);
      }
    }
    
    public void batchWrite(Context context) throws IOException {
      FileSystem fs = FileSystem.get(context.getConfiguration());
      String outputFolder = context.getConfiguration().get(OUTPUT_OPTION);
      if (!outputFolder.endsWith(File.separator))
        outputFolder += File.separator;
      Path p = new Path(outputFolder + (fileNumber++));
      FSDataOutputStream fsout = fs.create(p);
      BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fsout));

      for (String docid : docidMap.keySet()) {
        if (docidMap.get(docid).equals(EMPTY))
          continue;

        bw.write("<DOC>");
        bw.newLine();
        bw.write("<DOCNO>"+docid+"</DOCNO>");
        bw.newLine();
        bw.write("<TEXT>");
        bw.newLine();
        bw.write(docidMap.get(docid));
        bw.write("</TEXT>");
        bw.newLine();
        bw.write("</DOC>");
        bw.newLine();
      }
      bw.close();
      fsout.close();
      fs.close();
    }    

    @Override
    public void cleanup(Context context) throws IOException {
      batchWrite(context);
    }
  }

  public static final String INPUT_OPTION = "input";
  public static final String OUTPUT_OPTION = "output";
  public static final String DOCIDS_FILE = "docidsfile";
  public static final String KEEP_HTML = "keephtml";
  public static final String HTML_PARSER = "htmlParser";

  /**
   * Runs this tool.
   */
  @SuppressWarnings("static-access")
  public int run(String[] args) throws Exception {
    Options options = new Options();

    options.addOption(OptionBuilder.withArgName("path").hasArg().withDescription("input path")
        .create(INPUT_OPTION));
    options.addOption(OptionBuilder.withArgName("path").hasArg().withDescription("output path")
        .create(OUTPUT_OPTION));
    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("docids file path").create(DOCIDS_FILE));
    options.addOption(OptionBuilder.withArgName("true|false").hasArg().withDescription("keep HTML")
        .create(KEEP_HTML));
    options.addOption(OptionBuilder.withArgName("string " + HTMLParserFactory.getOptions())
        .hasArg().withDescription("htmlParser").create(HTML_PARSER));

    CommandLine cmdline;
    CommandLineParser parser = new GnuParser();
    try {
      cmdline = parser.parse(options, args);
    } catch (ParseException exp) {
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp(this.getClass().getName(), options);
      ToolRunner.printGenericCommandUsage(System.out);
      System.err.println("Error parsing command line: " + exp.getMessage());
      return -1;
    }

    if (!cmdline.hasOption(INPUT_OPTION) || !cmdline.hasOption(OUTPUT_OPTION)
         || !cmdline.hasOption(KEEP_HTML)) {
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp(this.getClass().getName(), options);
      ToolRunner.printGenericCommandUsage(System.out);
      return -1;
    }

    String input = cmdline.getOptionValue(INPUT_OPTION);
    String output = cmdline.getOptionValue(OUTPUT_OPTION);
    String docidsfile = cmdline.getOptionValue(DOCIDS_FILE);
    boolean keephtml = (cmdline.getOptionValue(KEEP_HTML).equals("true")) ? true : false;
    String htmlParser = (keephtml == false) ? cmdline.getOptionValue(HTML_PARSER) : "";

    LOG.info("Tool name: " + DocumentExtractor.class.getSimpleName());
    LOG.info(" - input: " + input);
    LOG.info(" - output: " + output);
    LOG.info(" - docidsfile: " + docidsfile);
    LOG.info(" - keephtml: " + keephtml);
    LOG.info(" - htmlParser: " + htmlParser);

    Configuration conf = getConf();
    conf.set(DOCIDS_FILE, docidsfile);
    conf.setBoolean(KEEP_HTML, keephtml);
    conf.set(OUTPUT_OPTION, output);
    conf.set(HTML_PARSER, htmlParser);

    Job job = new Job(getConf(), DocumentExtractor.class.getSimpleName() + ":" + input);
    job.setJarByClass(DocumentExtractor.class);

    FileInputFormat.setInputPaths(job, input);

    job.setInputFormatClass(ClueWeb12InputFormat.class);
    job.setOutputFormatClass(NullOutputFormat.class);

    job.setMapOutputKeyClass(NullWritable.class);
    job.setMapOutputValueClass(NullWritable.class);

    job.setMapperClass(MyMapper.class);
    job.setNumReduceTasks(0);

    long startTime = System.currentTimeMillis();
    job.waitForCompletion(true);
    LOG.info("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");

    int numDocsFound = (int) job.getCounters().findCounter(Records.DOCUMENTS_FOUND).getValue();
    LOG.info("Number of documents found: " + numDocsFound);

    int numExceptions = (int) job.getCounters().findCounter(Records.HTML_PARSER_EXCEPTIONS)
        .getValue();
    LOG.info("Number of HTML parser exceptions: " + numExceptions);

    return 0;
  }

  /**
   * Dispatches command-line arguments to the tool via the <code>ToolRunner</code>.
   */
  public static void main(String[] args) throws Exception {
    LOG.info("Running " + DocumentExtractor.class.getCanonicalName() + " with args "
        + Arrays.toString(args));
    ToolRunner.run(new DocumentExtractor(), args);
  }
}
