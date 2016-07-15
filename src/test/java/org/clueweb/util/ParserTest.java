package org.clueweb.util;

import org.junit.Test;
import static org.junit.Assert.assertEquals;

public class ParserTest {
  
  private static final String HTML1 = "<html><head><title>My title</title><body>My body</body></html>";
  private static final String HTML2 = "<html><head><title>My title</title><body><span>My</span> <div>body</div></body></html>";
  private static final String HTML3 = "<!DOCTYPE html PUBLIC \"-//W3C//DTD HTML 4.01 Transitional//EN\" \"http://www.w3.org/TR/html4/loose.dtd\"> <HTML> <HEAD><title>My title</title><meta name=\"description\" content=\"My title meta description\"></meta></head><body>My body</body></html>";
  
  
  
  @Test
  public void test1() throws Exception {
    
    String textJ = HTMLParserFactory.parse("jsoup", HTML1);
    assertEquals("My body", textJ.trim());
    textJ = HTMLParserFactory.parse("jsoup", HTML2);
    assertEquals("My body", textJ.trim());
    textJ = HTMLParserFactory.parse("jsoup", HTML3);
    assertEquals("My body", textJ.trim());
    
    String textT = HTMLParserFactory.parse("tika", HTML1);
    assertEquals("My body", textT.trim());
    textT = HTMLParserFactory.parse("tika", HTML2);
    assertEquals("My body", textT.trim());
    textT = HTMLParserFactory.parse("tika", HTML3);
    assertEquals("My body", textT.trim());
  }
  
  @Test
  public void test2() throws Exception {
    
    String[] textJ = HTMLParserFactory.parseTitleAndBody("jsoup", HTML1);
    assertEquals("My title", textJ[0].trim());
    assertEquals("My body", textJ[1].trim());
    textJ = HTMLParserFactory.parseTitleAndBody("jsoup", HTML2);
    assertEquals("My title", textJ[0].trim());
    assertEquals("My body", textJ[1].trim());
    textJ = HTMLParserFactory.parseTitleAndBody("jsoup", HTML3);
    assertEquals("My title", textJ[0].trim());
    assertEquals("My body", textJ[1].trim());
    
    String[] textT = HTMLParserFactory.parseTitleAndBody("tika", HTML1);
    assertEquals("My title", textT[0].trim());
    assertEquals("My body", textT[1].trim());
    textT = HTMLParserFactory.parseTitleAndBody("tika", HTML2);
    assertEquals("My title", textT[0].trim());
    assertEquals("My body", textT[1].trim());
    textT = HTMLParserFactory.parseTitleAndBody("tika", HTML3);
    assertEquals("My title", textT[0].trim());
    assertEquals("My body", textT[1].trim());
  }  

}
