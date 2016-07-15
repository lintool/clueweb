package org.clueweb.util;

import java.io.ByteArrayInputStream;

import org.apache.tika.metadata.Metadata;
import org.apache.tika.metadata.TikaCoreProperties;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.html.BoilerpipeContentHandler;
import org.apache.tika.parser.html.HtmlParser;
import org.apache.tika.sax.BodyContentHandler;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.xml.sax.ContentHandler;

import de.l3s.boilerpipe.BoilerpipeProcessingException;
import de.l3s.boilerpipe.extractors.ArticleExtractor;

public class HTMLParserFactory {

  /*
   * this method returns the content of the <body> tag 
   */
  public static String parse(String parserType, String content) throws Exception {
    
    if (parserType.equals("jsoup")) {
      return Jsoup.parse(content).body().text();
    }
    else if (parserType.equals("tika")) {
      ContentHandler handler = new BodyContentHandler();
      Metadata metadata = new Metadata();
      new HtmlParser().parse(new ByteArrayInputStream(content.getBytes("UTF-8")), handler,
          metadata, new ParseContext());
      return handler.toString();
    }
    else {
      return "";
    }
  }
  
  /*
   * this method returns the title and the content of the <body> tag in an array
   */
  public static String[] parseTitleAndBody(String parserType, String content) throws Exception {
    String[] result = new String[2];
    
    if (parserType.equals("jsoup")) {
      Document doc = Jsoup.parse(content);
      result[0] = doc.title();
      result[1] = doc.body().text();
    }
    else if (parserType.equals("tika")) {
      ContentHandler handler = new BodyContentHandler();
      Metadata metadata = new Metadata();
      new HtmlParser().parse(new ByteArrayInputStream(content.getBytes("UTF-8")), handler,
          metadata, new ParseContext());
      result[0] = metadata.get(TikaCoreProperties.TITLE); 
      result[1] = handler.toString();
    }
    else
      ;
    
    return result;
  }

  public static String getOptions() {
    return "jsoup|tika";
  }
}
