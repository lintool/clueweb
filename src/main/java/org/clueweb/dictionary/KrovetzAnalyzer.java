package org.clueweb.dictionary;

import java.io.IOException;
import java.io.Reader;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.core.LowerCaseFilter;
import org.apache.lucene.analysis.core.StopFilter;
import org.apache.lucene.analysis.en.KStemFilter;
import org.apache.lucene.analysis.standard.StandardFilter;
import org.apache.lucene.analysis.standard.StandardTokenizer;
import org.apache.lucene.analysis.util.CharArraySet;
import org.apache.lucene.analysis.util.StopwordAnalyzerBase;
import org.apache.lucene.util.Version;

import com.google.common.collect.Lists;

/**
 * Filters {@link StandardTokenizer} with {@link StandardFilter}, {@link LowerCaseFilter},
 * {@link StopFilter}, and {@link KStemFilter}.
 */
public final class KrovetzAnalyzer extends StopwordAnalyzerBase {

  // Stopwords used in the baseline run of TREC 2013 Web track.
  static final String[] STOPWORDS = { "a", "about", "above", "according", "across", "after",
      "afterwards", "again", "against", "albeit", "all", "almost", "alone", "along", "already",
      "also", "although", "always", "am", "among", "amongst", "an", "and", "another", "any",
      "anybody", "anyhow", "anyone", "anything", "anyway", "anywhere", "apart", "are", "around",
      "as", "at", "av", "be", "became", "because", "become", "becomes", "becoming", "been",
      "before", "beforehand", "behind", "being", "below", "beside", "besides", "between", "beyond",
      "both", "but", "by", "can", "cannot", "canst", "certain", "cf", "choose", "contrariwise",
      "cos", "could", "cu", "day", "do", "does", "doesn't", "doing", "dost", "doth", "double",
      "down", "dual", "during", "each", "either", "else", "elsewhere", "enough", "et", "etc",
      "even", "ever", "every", "everybody", "everyone", "everything", "everywhere", "except",
      "excepted", "excepting", "exception", "exclude", "excluding", "exclusive", "far", "farther",
      "farthest", "few", "ff", "first", "for", "formerly", "forth", "forward", "from", "front",
      "further", "furthermore", "furthest", "get", "go", "had", "halves", "hardly", "has", "hast",
      "hath", "have", "he", "hence", "henceforth", "her", "here", "hereabouts", "hereafter",
      "hereby", "herein", "hereto", "hereupon", "hers", "herself", "him", "himself", "hindmost",
      "his", "hither", "hitherto", "how", "however", "howsoever", "i", "ie", "if", "in",
      "inasmuch", "inc", "include", "included", "including", "indeed", "indoors", "inside",
      "insomuch", "instead", "into", "inward", "inwards", "is", "it", "its", "itself", "just",
      "kind", "kg", "km", "last", "latter", "latterly", "less", "lest", "let", "like", "little",
      "ltd", "many", "may", "maybe", "me", "meantime", "meanwhile", "might", "moreover", "most",
      "mostly", "more", "mr", "mrs", "ms", "much", "must", "my", "myself", "namely", "need",
      "neither", "never", "nevertheless", "next", "no", "nobody", "none", "nonetheless", "noone",
      "nope", "nor", "not", "nothing", "notwithstanding", "now", "nowadays", "nowhere", "of",
      "off", "often", "ok", "on", "once", "one", "only", "onto", "or", "other", "others",
      "otherwise", "ought", "our", "ours", "ourselves", "out", "outside", "over", "own", "per",
      "perhaps", "plenty", "provide", "quite", "rather", "really", "round", "said", "sake", "same",
      "sang", "save", "saw", "see", "seeing", "seem", "seemed", "seeming", "seems", "seen",
      "seldom", "selves", "sent", "several", "shalt", "she", "should", "shown", "sideways",
      "since", "slept", "slew", "slung", "slunk", "smote", "so", "some", "somebody", "somehow",
      "someone", "something", "sometime", "sometimes", "somewhat", "somewhere", "spake", "spat",
      "spoke", "spoken", "sprang", "sprung", "stave", "staves", "still", "such", "supposing",
      "than", "that", "the", "thee", "their", "them", "themselves", "then", "thence",
      "thenceforth", "there", "thereabout", "thereabouts", "thereafter", "thereby", "therefore",
      "therein", "thereof", "thereon", "thereto", "thereupon", "these", "they", "this", "those",
      "thou", "though", "thrice", "through", "throughout", "thru", "thus", "thy", "thyself",
      "till", "to", "together", "too", "toward", "towards", "ugh", "unable", "under", "underneath",
      "unless", "unlike", "until", "up", "upon", "upward", "upwards", "us", "use", "used", "using",
      "very", "via", "vs", "want", "was", "we", "week", "well", "were", "what", "whatever",
      "whatsoever", "when", "whence", "whenever", "whensoever", "where", "whereabouts",
      "whereafter", "whereas", "whereat", "whereby", "wherefore", "wherefrom", "wherein",
      "whereinto", "whereof", "whereon", "wheresoever", "whereto", "whereunto", "whereupon",
      "wherever", "wherewith", "whether", "whew", "which", "whichever", "whichsoever", "while",
      "whilst", "whither", "who", "whoa", "whoever", "whole", "whom", "whomever", "whomsoever",
      "whose", "whosoever", "why", "will", "wilt", "with", "within", "without", "worse", "worst",
      "would", "wow", "ye", "yet", "year", "yippee", "you", "your", "yours", "yourself",
      "yourselves", };

  /** Default maximum allowed token length */
  public static final int DEFAULT_MAX_TOKEN_LENGTH = 255;

  private int maxTokenLength = DEFAULT_MAX_TOKEN_LENGTH;

  public static final CharArraySet STOP_WORDS_SET = new CharArraySet(Version.LUCENE_43,
      Lists.newArrayList(STOPWORDS), true);

  public KrovetzAnalyzer() {
    super(Version.LUCENE_43, STOP_WORDS_SET);
  }

  /**
   * Set maximum allowed token length. If a token is seen that exceeds this length then it is
   * discarded. This setting only takes effect the next time tokenStream or tokenStream is called.
   */
  public void setMaxTokenLength(int length) {
    maxTokenLength = length;
  }

  public int getMaxTokenLength() {
    return maxTokenLength;
  }

  @Override
  protected TokenStreamComponents createComponents(final String fieldName, final Reader reader) {
    final StandardTokenizer src = new StandardTokenizer(matchVersion, reader);
    src.setMaxTokenLength(maxTokenLength);
    TokenStream tok = new StandardFilter(matchVersion, src);
    tok = new LowerCaseFilter(matchVersion, tok);
    tok = new StopFilter(matchVersion, tok, stopwords);
    tok = new KStemFilter(tok);
    return new TokenStreamComponents(src, tok) {
      @Override
      protected void setReader(final Reader reader) throws IOException {
        src.setMaxTokenLength(KrovetzAnalyzer.this.maxTokenLength);
        super.setReader(reader);
      }
    };
  }
}
