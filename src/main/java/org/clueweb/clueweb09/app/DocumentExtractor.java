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
 *  - If docidsfile is "-1", all encountered content is written to files (in batches of MAX_DOCS_IN_FILE docs per file)
 *  
 * MyMapper:
 * 	2.1 the docids are read from the file in setup()
 * 	2.2 all *warc.gz files are read in map() and if the right docid is hit, the content is stored
 * 	2.3 the cleanup() method writes the content of all batched docids to file
 * 
 * Reducer: no reducer necessary, all work is done in MyMapper
 */

package org.clueweb.clueweb09.app;

import info.debatty.java.stringsimilarity.KShingling;
import info.debatty.java.stringsimilarity.StringProfile;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Vector;

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
import org.clueweb.clueweb09.ClueWeb09WarcRecord;
import org.clueweb.clueweb09.mapreduce.ClueWeb09InputFormat;
import org.clueweb.util.HTMLParserFactory;

import com.google.common.collect.Maps;

public class DocumentExtractor extends Configured implements Tool {
  private static final Logger LOG = Logger.getLogger(DocumentExtractor.class);

  private static enum Records {
    DOCUMENTS_FOUND, HTML_PARSER_EXCEPTIONS, NON_EMPTY_TITLES, DOCUMENTS_WRITTEN_TO_FILE, DUPLICATES_FOUND
  };

  private static boolean removeDuplicates;
  private static boolean keepHTML;
  private static String htmlParser;
  private static boolean writeAll;
  private static final String EMPTY = "";
  private static final int MAX_DOCS_IN_FILE = 1000;//number of clueweb docs written to a single file
  private static final double COSINE_DUPLICATION_THRESHOLD = 0.9; //remove duplicates (documents with a higher similarity threshold than given here)
  
  private static class MyMapper extends
      Mapper<LongWritable, ClueWeb09WarcRecord, NullWritable, NullWritable> {

    private HashMap<String, String> docidMap;//document content (inside <body>) 
    private HashMap<String, String> titleMap;//document titles (inside <title>)  
    
    @Override
    public void setup(Context context) throws IOException {

      docidMap = Maps.newHashMap();//document content (inside <body>) 
      titleMap = Maps.newHashMap();//document titles (inside <title>)
      
      FileSystem fs = FileSystem.get(context.getConfiguration());
      String docidsFile = context.getConfiguration().get(DOCIDS_FILE);
      
      writeAll = (docidsFile.equals("-1")) ? true : false;
      keepHTML = context.getConfiguration().getBoolean(KEEP_HTML, true);
      htmlParser = context.getConfiguration().get(HTML_PARSER);
      removeDuplicates = context.getConfiguration().getBoolean(REMOVE_DUPLICATES,false);

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
    public void map(LongWritable key, ClueWeb09WarcRecord doc, Context context) throws IOException,
        InterruptedException {

      try {
        //from time to time write the hashmap content to file
        if(docidMap.size()>MAX_DOCS_IN_FILE) {
          batchWrite(context);
        }
    
        String docid = doc.getHeaderMetadataItem("WARC-TREC-ID").toLowerCase();
        if (docid != null && (docidMap.containsKey(docid) || writeAll==true) ) {

          if (keepHTML==false) {
            String[] titleBody = HTMLParserFactory.parseTitleAndBody(htmlParser, doc.getContent());
            titleMap.put(docid, titleBody[0]);
            if(titleBody[0].length()>0) {
              context.getCounter(Records.NON_EMPTY_TITLES).increment(1);
            }
            docidMap.put(docid, titleBody[1]);
          } else {
            docidMap.put(docid, doc.getContent());//entire HTML of the document
          }
          context.getCounter(Records.DOCUMENTS_FOUND).increment(1);
        } 
      }
      catch (Exception e) {
        // If the HTML parser throws any exceptions, catch and move on.
        LOG.info("Error caught when processing a document");
        context.getCounter(Records.HTML_PARSER_EXCEPTIONS).increment(1);
      }
    }
    
    public void batchWrite(Context context) throws IOException {
      FileSystem fs = FileSystem.get(context.getConfiguration());
      String outputFolder = context.getConfiguration().get(OUTPUT_OPTION);
      if (!outputFolder.endsWith(File.separator))
        outputFolder += File.separator;
      
      long timestamp1 = System.currentTimeMillis();
      long timestamp2 = System.currentTimeMillis();
      Path p = new Path(outputFolder + timestamp1 + "_" + timestamp2);
      FSDataOutputStream fsout = fs.create(p);
      BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fsout));
      
      //one-off
      bw.write("<add>");
      bw.newLine();
      
      Vector<StringProfile> shinglings = new Vector<StringProfile>();
      KShingling ks = new KShingling(5);
      
      for (String docid : docidMap.keySet()) {
        if (docidMap.get(docid).equals(EMPTY))
          continue;

        boolean isDuplicate = false;
        
        if(removeDuplicates == true) {
          StringProfile sp = ks.getProfile(docidMap.get(docid));
          for(StringProfile s : shinglings) {
            try {
              if( sp.cosineSimilarity(s) > COSINE_DUPLICATION_THRESHOLD) {
                isDuplicate = true;
                break;
              }
            } catch (Exception e) {
              ;
            }
          }
          
          //if the document is not a duplicate, add its profile to the shinglings written out to file
          if(isDuplicate == false) {
            shinglings.add(sp);
          }
        }
        
        //if we have a duplicate, we do not write it out to file
        if(isDuplicate == true) {
          context.getCounter(Records.DUPLICATES_FOUND).increment(1);
          continue;
        }
        
        context.getCounter(Records.DOCUMENTS_WRITTEN_TO_FILE).increment(1);
        bw.write("<doc>");
        bw.write("<field name=\"id\">"+docid+"</field>");bw.newLine();
        bw.write("<field name=\"duplicate\">false</field>"); bw.newLine();
        bw.write("<field name=\"name\">"+titleMap.get(docid)+"</field>"); bw.newLine();
        bw.write("<field name=\"document\">"); bw.write(docidMap.get(docid)); bw.write("</field></doc>");
        bw.newLine();
      }
      //one-off
      bw.write("</add>");
      bw.newLine();
      bw.close();
      fsout.close();
      
      docidMap.clear();
      titleMap.clear();
      shinglings.clear();
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
  public static final String REMOVE_DUPLICATES = "removeDuplicates";
  
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
    options.addOption(OptionBuilder.withArgName("true|false").hasArg().withDescription("remove duplicates")
        .create(REMOVE_DUPLICATES));
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
         || !cmdline.hasOption(KEEP_HTML) || !cmdline.hasOption(REMOVE_DUPLICATES)) {
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp(this.getClass().getName(), options);
      ToolRunner.printGenericCommandUsage(System.out);
      return -1;
    }

    String input = cmdline.getOptionValue(INPUT_OPTION);
    String output = cmdline.getOptionValue(OUTPUT_OPTION);
    String docidsfile = cmdline.getOptionValue(DOCIDS_FILE);
    boolean keephtml = (cmdline.getOptionValue(KEEP_HTML).equals("true")) ? true : false;
    boolean removeDuplicates = (cmdline.getOptionValue(REMOVE_DUPLICATES).equals("true")) ? true : false;
    String htmlParser = (keephtml == false) ? cmdline.getOptionValue(HTML_PARSER) : "";

    LOG.info("Tool name: " + DocumentExtractor.class.getSimpleName());
    LOG.info(" - input: " + input);
    LOG.info(" - output: " + output);
    LOG.info(" - docidsfile: " + docidsfile);
    LOG.info(" - keephtml: " + keephtml);
    LOG.info(" - htmlParser: " + htmlParser);
    LOG.info(" - removeDuplicates: " + removeDuplicates);

    Configuration conf = getConf();
    conf.set(DOCIDS_FILE, docidsfile);
    conf.setBoolean(KEEP_HTML, keephtml);
    conf.setBoolean(REMOVE_DUPLICATES, removeDuplicates);
    conf.set(OUTPUT_OPTION, output);
    conf.set(HTML_PARSER, htmlParser);

    Job job = new Job(getConf(), DocumentExtractor.class.getSimpleName() + ":" + input);
    job.setJarByClass(DocumentExtractor.class);

    FileInputFormat.setInputPaths(job, input);

    job.setInputFormatClass(ClueWeb09InputFormat.class);
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
    
    int numDocsWritten = (int) job.getCounters().findCounter(Records.DOCUMENTS_WRITTEN_TO_FILE)
        .getValue();
    LOG.info("Number of documents written to file: " + numDocsWritten);
    
    int numDuplicatesFound = (int) job.getCounters().findCounter(Records.DUPLICATES_FOUND)
        .getValue();
    LOG.info("Number of duplicates not written to file: " + numDuplicatesFound);

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
