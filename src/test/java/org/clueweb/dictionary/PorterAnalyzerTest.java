package org.clueweb.dictionary;

import static org.junit.Assert.assertEquals;

import java.util.List;

import junit.framework.JUnit4TestAdapter;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.util.Version;
import org.junit.Test;

import tl.lin.lucene.AnalyzerUtils;

import com.google.common.base.Joiner;

public class PorterAnalyzerTest {
  @Test
  public void test1() throws Exception {
    Analyzer analyzer = new PorterAnalyzer();
    List<String> tokens = AnalyzerUtils.parse(analyzer,
        "The U.S. Dept. of Justice has announced that Panasonic and its subsidiary Sanyo have been fined $56.5 million for their roles in price fixing conspiracies involving battery cells and car parts.");

    System.out.println(Joiner.on(",").join(tokens));
    assertEquals(19, tokens.size());
  }

  @Test
  public void test2() throws Exception {
    Analyzer analyzer = new StandardAnalyzer(Version.LUCENE_43);
    List<String> tokens = AnalyzerUtils.parse(analyzer,
        "The U.S. Dept. of Justice has announced that Panasonic and its subsidiary Sanyo have been fined $56.5 million for their roles in price fixing conspiracies involving battery cells and car parts.");

    System.out.println(Joiner.on(",").join(tokens));
    assertEquals(23, tokens.size());
  }

  public static junit.framework.Test suite() {
    return new JUnit4TestAdapter(PorterAnalyzerTest.class);
  }
}