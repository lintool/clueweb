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

package org.clueweb.util;

import static org.junit.Assert.assertEquals;


import java.util.List;

import junit.framework.JUnit4TestAdapter;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.util.Version;
import org.clueweb.dictionary.KrovetzAnalyzer;
import org.clueweb.dictionary.PorterAnalyzer;
import org.junit.Test;

import tl.lin.lucene.AnalyzerUtils;

import com.google.common.base.Joiner;

public class AnalyzerTest {
  @Test
  public void test1() throws Exception {
    Analyzer analyzer = new PorterAnalyzer();
    List<String> tokens = AnalyzerUtils
        .parse(
            analyzer,
            "The U.S. Dept. of Justice has announced that Panasonic and its subsidiary Sanyo have been fined $56.5 million for their roles in price fixing conspiracies involving battery cells and car parts.");

    System.out.println(Joiner.on(",").join(tokens));
    assertEquals(19, tokens.size());
  }

  @Test
  public void test2() throws Exception {
    Analyzer analyzer = new KrovetzAnalyzer();
    List<String> tokens = AnalyzerUtils
        .parse(
            analyzer,
            "The U.S. Dept. of Justice has announced that Panasonic and its subsidiary Sanyo have been fined $56.5 million for their roles in price fixing conspiracies involving battery cells and car parts.");

    System.out.println(Joiner.on(",").join(tokens));
    assertEquals(19, tokens.size());
  }

  @Test
  public void test3() throws Exception {
    Analyzer analyzer = new StandardAnalyzer(Version.LUCENE_43);
    List<String> tokens = AnalyzerUtils
        .parse(
            analyzer,
            "The U.S. Dept. of Justice has announced that Panasonic and its subsidiary Sanyo have been fined $56.5 million for their roles in price fixing conspiracies involving battery cells and car parts.");

    System.out.println(Joiner.on(",").join(tokens));
    assertEquals(23, tokens.size());
  }

  public static junit.framework.Test suite() {
    return new JUnit4TestAdapter(AnalyzerTest.class);
  }
}