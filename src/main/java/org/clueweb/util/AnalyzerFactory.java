package org.clueweb.util;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.util.Version;
import org.clueweb.dictionary.PorterAnalyzer;

public class AnalyzerFactory {

  public static Analyzer getAnalyzer(String analyzerType) {
    if (analyzerType.equals("standard")) {
      return new org.apache.lucene.analysis.standard.StandardAnalyzer(Version.LUCENE_43);
    }

    if (analyzerType.equals("porter")) {
      return new PorterAnalyzer();
    }

    return null;
  }

  public static String getOptions() {
    return "standard|porter";
  }
}