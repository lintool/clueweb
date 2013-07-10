package org.clueweb.clueweb12.lucene;

import java.io.IOException;
import java.io.StringReader;
import java.util.List;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;

import com.google.common.collect.Lists;

public class AnalyzerUtil {
  static public List<String> parse(Analyzer analyzer, String keywords) throws IOException {
    List<String> list = Lists.newArrayList();

    TokenStream tokenStream = analyzer.tokenStream(null, new StringReader(keywords));
    CharTermAttribute cattr = tokenStream.addAttribute(CharTermAttribute.class);
    tokenStream.reset();
    while (tokenStream.incrementToken()) {
      if (cattr.toString().length() == 0) {
        continue;
      }
      list.add(cattr.toString());
    }
    tokenStream.end();
    tokenStream.close();

    return list;
  }
}
