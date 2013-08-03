package org.clueweb.util;

import java.io.ByteArrayInputStream;

import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.html.BoilerpipeContentHandler;
import org.apache.tika.parser.html.HtmlParser;
import org.apache.tika.sax.BodyContentHandler;
import org.jsoup.Jsoup;
import org.xml.sax.ContentHandler;

import de.l3s.boilerpipe.BoilerpipeProcessingException;
import de.l3s.boilerpipe.extractors.ArticleExtractor;

public class HTMLParserFactory {

  public static String parse(String parserType, String content) throws Exception {
    if (parserType.equals("jsoup")) {
      return Jsoup.parse(content).text();
    }

    if (parserType.equals("tika")) {
      ContentHandler handler = new BodyContentHandler();
      Metadata metadata = new Metadata();
      new HtmlParser().parse(new ByteArrayInputStream(content.getBytes("UTF-8")), handler,
          metadata, new ParseContext());
      return handler.toString();
    }

    if (parserType.startsWith("boilerpipe")) {
      Metadata metadata = new Metadata();
      ContentHandler handler = new BodyContentHandler();
      
      BoilerpipeContentHandler bpch = null;
      
      if(parserType.equals("boilerpipe"))
        bpch = new BoilerpipeContentHandler(handler,new de.l3s.boilerpipe.extractors.DefaultExtractor());
      else if(parserType.endsWith("-article"))
        bpch = new BoilerpipeContentHandler(handler,  new de.l3s.boilerpipe.extractors.ArticleExtractor());
      else if(parserType.endsWith("-keepEverythingMin100"))
        bpch = new BoilerpipeContentHandler(handler,new de.l3s.boilerpipe.extractors.KeepEverythingWithMinKWordsExtractor(100));
      else
        return null;
      
      bpch.setIncludeMarkup(false);
      new HtmlParser().parse(new ByteArrayInputStream(content.getBytes("UTF-8")), bpch, metadata,
          new ParseContext());
      return bpch.toString();
    }

    return null;
  }

  public static String getOptions() {
    return "jsoup|tika|boilerpipe|boilerpipe-article|boilerpipe-keepEverythingMin100";
  }
}
