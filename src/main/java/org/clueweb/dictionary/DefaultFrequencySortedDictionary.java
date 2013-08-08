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

package org.clueweb.dictionary;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.clueweb.clueweb12.app.BuildDictionary;
import org.clueweb.data.TermStatistics;

/**
 * An implementation of {@link FrequencySortedDictionary}. Term ids start at 1, which corresponds to
 * the most frequent term. Term id 2 is the second most frequent term, etc.
 * 
 * @author Jimmy Lin
 */
public class DefaultFrequencySortedDictionary implements FrequencySortedDictionary {
  private FrontCodedDictionary dictionary = new FrontCodedDictionary();
  private int[] ids;
  private int[] idsToTerm;

  /**
   * Constructs an instance of this dictionary from serialized data files.
   */
  public DefaultFrequencySortedDictionary(String basePath, FileSystem fs) throws IOException {
    FSDataInputStream in;

    in = fs.open(new Path(basePath, BuildDictionary.TERMS_DATA));
    dictionary.readFields(in);
    in.close();

    int l = 0;

    in = fs.open(new Path(basePath, BuildDictionary.TERMS_ID_DATA));
    l = in.readInt();
    ids = new int[l];
    for (int i = 0; i < l; i++) {
      ids[i] = in.readInt();
    }
    in.close();

    in = fs.open(new Path(basePath, BuildDictionary.TERMS_ID_MAPPING_DATA));
    l = in.readInt();
    idsToTerm = new int[l];
    for (int i = 0; i < l; i++) {
      idsToTerm[i] = in.readInt();
    }
    in.close();
  }

  @Override
  public int size() {
    return ids.length;
  }

  @Override
  public int getId(String term) {
    int index = dictionary.getId(term);

    if (index < 0) {
      return -1;
    }

    return ids[index];
  }

  @Override
  public String getTerm(int id) {
    if (id > ids.length || id == 0 || idsToTerm == null) {
      return null;
    }
    String term = dictionary.getTerm(idsToTerm[id - 1]);

    return term;
  }

  /**
   * Returns an iterator over the dictionary in order of term id.
   */
  @Override
  public Iterator<String> iterator() {
    return new Iterator<String>() {
      private int cur = 1;
      final private int end = dictionary.size();

      @Override
      public boolean hasNext() {
        return cur < end + 1;
      }

      @Override
      public String next() {
        return getTerm(cur++);
      }

      @Override
      public void remove() {
        throw new UnsupportedOperationException();
      }
    };
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
    FileSystem fs = FileSystem.get(conf);

    DefaultFrequencySortedDictionary dictionary = new DefaultFrequencySortedDictionary(path, fs);

    int nTerms = dictionary.size();
    out.println("number of terms: " + nTerms);

    TermStatistics stats = new TermStatistics(new Path(path), fs);
    out.println("max df = " + stats.getMaxDf() + ", termid " + stats.getMaxDfTerm());
    out.println("max cf = " + stats.getMaxCf() + ", termid " + stats.getMaxCfTerm());
    out.println("collection size = " + stats.getCollectionSize());
    out.println("");

    out.println(" \"term word\" to lookup termid; \"termid 234\" to lookup term");
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

      if (tokens[0].equals("termid")) {
        int termid;
        try {
          termid = Integer.parseInt(tokens[1]);
        } catch (Exception e) {
          out.println("Error: invalid termid!");
          out.print("lookup > ");

          continue;
        }

        out.println("termid=" + termid + ", term=" + dictionary.getTerm(termid));
        out.println("  df = " + stats.getDf(termid) + ", cf = " + stats.getCf(termid));
      } else if (tokens[0].equals("term")) {
        String term = tokens[1];

        out.println("term=" + term + ", termid=" + dictionary.getId(term));
        out.println("  df = " + stats.getDf(dictionary.getId(term)) + ", cf = "
            + stats.getCf(dictionary.getId(term)));
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
