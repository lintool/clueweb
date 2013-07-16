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

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.WritableUtils;
import org.clueweb.clueweb12.app.BuildDictionary;

import com.google.common.base.Preconditions;

public class SelectedTermStatistics {
  private final int numTerms;

  private long collectionSize;

  private long maxCf = 0;
  private int maxCfTerm;

  private int maxDf = 0;
  private int maxDfTerm;
  
  public SelectedTermStatistics(Path file,HashMap<Integer, Long> cfMap, HashMap<Integer,Integer> dfMap) throws IOException {
    this(file, FileSystem.get(new Configuration()),cfMap,dfMap);
  }

  /**
   * Instead of reading the collection statistics for all term,
   * only keep those we are actually interested in
   * (given as keys in cfMap/dfMap)
   */
  public SelectedTermStatistics(Path file, FileSystem fs, HashMap<Integer, Long> cfMap, HashMap<Integer,Integer> dfMap) throws IOException {
    Preconditions.checkNotNull(file);
    Preconditions.checkNotNull(fs);

    FSDataInputStream in = fs.open(new Path(file, BuildDictionary.CF_BY_ID_DATA));
    this.numTerms = in.readInt();
    for (int i = 0; i < numTerms; i++) {
      long cf = WritableUtils.readVLong(in);
      int termid = i+1;      
      if(cfMap.containsKey(termid))
        cfMap.put(termid, cf);
      collectionSize += cf;
      if (cf > maxCf) {
        maxCf = cf;
        maxCfTerm = i + 1;
      }
    }

    in.close();

    in = fs.open(new Path(file, BuildDictionary.DF_BY_ID_DATA));
    if (numTerms != in.readInt() ) {
      throw new IOException("df data and cf data should have the same number of entries!");
    }
    
    for (int i = 0; i < numTerms; i++) {
      int df = WritableUtils.readVInt(in);
      int termid = i+1;
      if(dfMap.containsKey(termid))
        dfMap.put(termid, df);
      if (df > maxDf) {
        maxDf = df;
        maxDfTerm = i + 1;
      }
    }

    in.close();
  }


  public long getCollectionSize() {
    return collectionSize;
  }

  public int getVocabularySize() {
    return numTerms;
  }

  public int getMaxDf() {
    return maxDf;
  }

  public long getMaxCf() {
    return maxCf;
  }

  public int getMaxDfTerm() {
    return maxDfTerm;
  }

  public int getMaxCfTerm() {
    return maxCfTerm;
  }

}
