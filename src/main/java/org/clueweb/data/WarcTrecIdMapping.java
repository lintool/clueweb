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

package org.clueweb.data;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;

import tl.lin.lucene.FileSystemDirectory;

public class WarcTrecIdMapping {
  private static final Logger LOG = Logger.getLogger(WarcTrecIdMapping.class);

  public static enum IndexField {
    WARC_TREC_ID("WARC-TREC-ID");

    public final String name;

    IndexField(String s) {
      name = s;
    }
  };

  private IndexReader reader;
  private IndexSearcher searcher;

  public WarcTrecIdMapping(Path indexLocation, Configuration conf) throws IOException {
    FileSystem fs = FileSystem.get(conf);
    Directory directory = new FileSystemDirectory(fs, indexLocation, false, conf);

    LOG.info("Opening index " + indexLocation);
    reader = DirectoryReader.open(directory);
    searcher = new IndexSearcher(reader);
  }

  public int getDocno(String id) {
    Query query = new TermQuery(new Term(IndexField.WARC_TREC_ID.name, id));

    TopDocs rs;
    try {
      rs = searcher.search(query, 1);
      if (rs.totalHits != 1) {
        return -1;
      }

      return rs.scoreDocs[0].doc;
    } catch (IOException e) {
      e.printStackTrace();
    }

    return -1;
  }

  public String getDocid(int docno) {
    if (docno >= reader.maxDoc()) {
      return null;
    }
    try {
      Document d = reader.document(docno);
      if (d == null) {
        return null;
      }
      return d.getField(IndexField.WARC_TREC_ID.name).stringValue();
    } catch (IOException e) {
      e.printStackTrace();
    }
    return null;
  }

  /**
   * Simple demo program for looking up terms and term ids.
   */
  public static void main(String[] args) throws Exception {
    if (args.length != 1) {
      System.err.println("usage: [index-path]");
      System.exit(-1);
    }

    String path = args[0];

    PrintStream out = new PrintStream(System.out, true, "UTF-8");

    Configuration conf = new Configuration();
    WarcTrecIdMapping mapping = new WarcTrecIdMapping(new Path(path), conf);

    out.println(" \"docid\" to lookup docid; \"docno\" to lookup docno");
    String cmd = null;
    BufferedReader stdin = new BufferedReader(new InputStreamReader(System.in));
    out.print("lookup > ");
    while ((cmd = stdin.readLine()) != null) {

      String[] tokens = cmd.split("\\s+");

      if (tokens.length != 2) {
        out.println("Error: unrecognized command!");
        out.print("lookup > ");

        continue;
      }

      if (tokens[0].equals("docid")) {
        int docno;
        try {
          docno = Integer.parseInt(tokens[1]);
        } catch (Exception e) {
          out.println("Error: invalid docno!");
          out.print("lookup > ");

          continue;
        }

        out.println("docno=" + docno + ", docid=" + mapping.getDocid(docno));
      } else if (tokens[0].equals("docno")) {
        String docid = tokens[1];

        out.println("docno=" + mapping.getDocno(docid) + ", docid=" + docid);
      } else {
        out.println("Error: unrecognized command!");
        out.print("lookup > ");
        continue;
      }

      out.print("lookup > ");
    }
    out.close();
  }
}
