package org.clueweb.dictionary;

import java.io.IOException;
import java.io.Reader;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.core.LowerCaseFilter;
import org.apache.lucene.analysis.core.StopFilter;
import org.apache.lucene.analysis.en.PorterStemFilter;
import org.apache.lucene.analysis.standard.StandardFilter;
import org.apache.lucene.analysis.standard.StandardTokenizer;
import org.apache.lucene.analysis.util.CharArraySet;
import org.apache.lucene.analysis.util.StopwordAnalyzerBase;
import org.apache.lucene.util.Version;

import com.google.common.collect.Lists;

/**
 * Filters {@link StandardTokenizer} with {@link StandardFilter},
 * {@link LowerCaseFilter}, {@link StopFilter}, and {@link PorterStemFilter}.
 */
public final class PorterAnalyzer extends StopwordAnalyzerBase {

	// Stopwords from Terrier v3.5.
	static final String[] STOPWORDS = { "a", "abaft", "abafter", "abaftest",
			"about", "abouter", "aboutest", "above", "abover", "abovest",
			"accordingly", "aer", "aest", "afore", "after", "afterer",
			"afterest", "afterward", "afterwards", "again", "against", "aid",
			"ain", "albeit", "all", "aller", "allest", "alls", "allyou",
			"almost", "along", "alongside", "already", "also", "although",
			"always", "amid", "amidst", "among", "amongst", "an", "and",
			"andor", "anear", "anent", "another", "any", "anybody", "anyhow",
			"anyone", "anything", "anywhere", "apart", "aparter", "apartest",
			"appear", "appeared", "appearing", "appears", "appropriate",
			"appropriated", "appropriater", "appropriates", "appropriatest",
			"appropriating", "are", "ares", "around", "as", "ases", "aside",
			"asides", "aslant", "astraddle", "astraddler", "astraddlest",
			"astride", "astrider", "astridest", "at", "athwart", "atop",
			"atween", "aught", "aughts", "available", "availabler",
			"availablest", "awfully", "b", "be", "became", "because", "become",
			"becomes", "becoming", "becominger", "becomingest", "becomings",
			"been", "before", "beforehand", "beforehander", "beforehandest",
			"behind", "behinds", "below", "beneath", "beside", "besides",
			"better", "bettered", "bettering", "betters", "between", "betwixt",
			"beyond", "bist", "both", "but", "buts", "by", "by-and-by",
			"byandby", "c", "cannot", "canst", "cant", "canted", "cantest",
			"canting", "cants", "cer", "certain", "certainer", "certainest",
			"cest", "chez", "circa", "co", "come-on", "come-ons", "comeon",
			"comeons", "concerning", "concerninger", "concerningest",
			"consequently", "considering", "could", "couldst", "cum", "d",
			"dday", "ddays", "describe", "described", "describes",
			"describing", "despite", "despited", "despites", "despiting",
			"did", "different", "differenter", "differentest", "do", "doe",
			"does", "doing", "doings", "done", "doner", "dones", "donest",
			"dos", "dost", "doth", "downs", "downward", "downwarder",
			"downwardest", "downwards", "during", "e", "each", "eg", "eight",
			"either", "else", "elsewhere", "enough", "ere", "et", "etc",
			"even", "evened", "evenest", "evens", "evenser", "evensest",
			"ever", "every", "everybody", "everyone", "everything",
			"everywhere", "ex", "except", "excepted", "excepting", "excepts",
			"exes", "f", "fact", "facts", "failing", "failings", "few",
			"fewer", "fewest", "figupon", "figuponed", "figuponing",
			"figupons", "five", "followthrough", "for", "forby", "forbye",
			"fore", "forer", "fores", "forever", "former", "formerer",
			"formerest", "formerly", "formers", "fornenst", "forwhy", "four",
			"fourscore", "frae", "from", "fs", "further", "furthered",
			"furtherer", "furtherest", "furthering", "furthermore", "furthers",
			"g", "get", "gets", "getting", "go", "gone", "good", "got",
			"gotta", "gotten", "h", "had", "hadst", "hae", "hardly", "has",
			"hast", "hath", "have", "haves", "having", "he", "hence", "her",
			"hereafter", "hereafters", "hereby", "herein", "hereupon", "hers",
			"herself", "him", "himself", "his", "hither", "hitherer",
			"hitherest", "hoo", "hoos", "how", "how-do-you-do", "howbeit",
			"howdoyoudo", "however", "huh", "humph", "i", "idem", "idemer",
			"idemest", "ie", "if", "ifs", "immediate", "immediately",
			"immediater", "immediatest", "in", "inasmuch", "inc", "indeed",
			"indicate", "indicated", "indicates", "indicating", "info",
			"information", "insofar", "instead", "into", "inward", "inwarder",
			"inwardest", "inwards", "is", "it", "its", "itself", "j", "k", "l",
			"latter", "latterer", "latterest", "latterly", "latters",
			"layabout", "layabouts", "less", "lest", "lot", "lots", "lotted",
			"lotting", "m", "main", "make", "many", "mauger", "maugre",
			"mayest", "me", "meanwhile", "meanwhiles", "midst", "midsts",
			"might", "mights", "more", "moreover", "most", "mostly", "much",
			"mucher", "muchest", "must", "musth", "musths", "musts", "my",
			"myself", "n", "natheless", "nathless", "neath", "neaths",
			"necessarier", "necessariest", "necessary", "neither", "nethe",
			"nethermost", "never", "nevertheless", "nigh", "nigher", "nighest",
			"nine", "no", "no-one", "nobodies", "nobody", "noes", "none",
			"noone", "nor", "nos", "not", "nothing", "nothings",
			"notwithstanding", "nowhere", "nowheres", "o", "of", "off",
			"offest", "offs", "often", "oftener", "oftenest", "oh", "on",
			"one", "oneself", "onest", "ons", "onto", "or", "orer", "orest",
			"other", "others", "otherwise", "otherwiser", "otherwisest",
			"ought", "oughts", "our", "ours", "ourself", "ourselves", "out",
			"outed", "outest", "outs", "outside", "outwith", "over", "overall",
			"overaller", "overallest", "overalls", "overs", "own", "owned",
			"owning", "owns", "owt", "p", "particular", "particularer",
			"particularest", "particularly", "particulars", "per", "perhaps",
			"plaintiff", "please", "pleased", "pleases", "plenties", "plenty",
			"pro", "probably", "provide", "provided", "provides", "providing",
			"q", "qua", "que", "quite", "r", "rath", "rathe", "rather",
			"rathest", "re", "really", "regarding", "relate", "related",
			"relatively", "res", "respecting", "respectively", "s", "said",
			"saider", "saidest", "same", "samer", "sames", "samest", "sans",
			"sanserif", "sanserifs", "sanses", "saved", "sayid", "sayyid",
			"seem", "seemed", "seeminger", "seemingest", "seemings", "seems",
			"send", "sent", "senza", "serious", "seriouser", "seriousest",
			"seven", "several", "severaler", "severalest", "shall", "shalled",
			"shalling", "shalls", "she", "should", "shoulded", "shoulding",
			"shoulds", "since", "sine", "sines", "sith", "six", "so", "sobeit",
			"soer", "soest", "some", "somebody", "somehow", "someone",
			"something", "sometime", "sometimer", "sometimes", "sometimest",
			"somewhat", "somewhere", "stop", "stopped", "such", "summat",
			"sup", "supped", "supping", "sups", "syn", "syne", "t", "ten",
			"than", "that", "the", "thee", "their", "theirs", "them",
			"themselves", "then", "thence", "thener", "thenest", "there",
			"thereafter", "thereby", "therefore", "therein", "therer",
			"therest", "thereupon", "these", "they", "thine", "thing",
			"things", "this", "thises", "thorough", "thorougher",
			"thoroughest", "thoroughly", "those", "thou", "though", "thous",
			"thouses", "three", "thro", "through", "througher", "throughest",
			"throughout", "thru", "thruer", "thruest", "thus", "thy",
			"thyself", "till", "tilled", "tilling", "tills", "to", "together",
			"too", "toward", "towarder", "towardest", "towards", "two", "u",
			"umpteen", "under", "underneath", "unless", "unlike", "unliker",
			"unlikest", "until", "unto", "up", "upon", "uponed", "uponing",
			"upons", "upped", "upping", "ups", "us", "use", "used", "usedest",
			"username", "usually", "v", "various", "variouser", "variousest",
			"verier", "veriest", "versus", "very", "via", "vis-a-vis",
			"vis-a-viser", "vis-a-visest", "viz", "vs", "w", "was", "wast",
			"we", "were", "wert", "what", "whatever", "whateverer",
			"whateverest", "whatsoever", "whatsoeverer", "whatsoeverest",
			"wheen", "when", "whenas", "whence", "whencesoever", "whenever",
			"whensoever", "where", "whereafter", "whereas", "whereby",
			"wherefrom", "wherein", "whereinto", "whereof", "whereon",
			"wheresoever", "whereto", "whereupon", "wherever", "wherewith",
			"wherewithal", "whether", "which", "whichever", "whichsoever",
			"while", "whiles", "whilst", "whither", "whithersoever", "whoever",
			"whomever", "whose", "whoso", "whosoever", "why", "with", "withal",
			"within", "without", "would", "woulded", "woulding", "woulds", "x",
			"y", "ye", "yet", "yon", "yond", "yonder", "you", "your", "yours",
			"yourself", "yourselves", "z", "zillion", };

	/** Default maximum allowed token length */
	public static final int DEFAULT_MAX_TOKEN_LENGTH = 255;

	private int maxTokenLength = DEFAULT_MAX_TOKEN_LENGTH;

	public static final CharArraySet STOP_WORDS_SET = new CharArraySet(
			Version.LUCENE_43, Lists.newArrayList(STOPWORDS), true);

	public PorterAnalyzer() {
		super(Version.LUCENE_43, STOP_WORDS_SET);
	}

	/**
	 * Set maximum allowed token length. If a token is seen that exceeds this
	 * length then it is discarded. This setting only takes effect the next time
	 * tokenStream or tokenStream is called.
	 */
	public void setMaxTokenLength(int length) {
		maxTokenLength = length;
	}

	public int getMaxTokenLength() {
		return maxTokenLength;
	}

	@Override
	protected TokenStreamComponents createComponents(final String fieldName,
			final Reader reader) {
		final StandardTokenizer src = new StandardTokenizer(matchVersion,
				reader);
		src.setMaxTokenLength(maxTokenLength);
		TokenStream tok = new StandardFilter(matchVersion, src);
		tok = new LowerCaseFilter(matchVersion, tok);
		tok = new StopFilter(matchVersion, tok, stopwords);
		tok = new PorterStemFilter(tok);
		return new TokenStreamComponents(src, tok) {
			@Override
			protected void setReader(final Reader reader) throws IOException {
				src.setMaxTokenLength(PorterAnalyzer.this.maxTokenLength);
				super.setReader(reader);
			}
		};
	}
}
