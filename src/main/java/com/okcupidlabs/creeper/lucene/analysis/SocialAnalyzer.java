package com.okcupidlabs.creeper.lucene.analysis;

import java.io.Reader;
import java.io.StringReader;
import java.io.IOException;
import org.apache.lucene.util.Version;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardTokenizer;
import org.apache.lucene.analysis.WhitespaceTokenizer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;

public final class SocialAnalyzer extends Analyzer {
  
  @Override
  public TokenStream tokenStream(String fieldName, Reader reader) {
    // use the standard tokenizer, until it pisses us off.
    final Version matchVersion = Version.LUCENE_35;
    TokenStream ts = new WhitespaceTokenizer(matchVersion, reader);
    // pack on some filters here:
    // one to normalize
    ts = new NormalizeFilter(ts);
    // one to create n-grams that look like they might be entities
    // ts = new EntityFilter(ts);
    return ts;
  }
  
  public static void main(String[] argv) {
    // instantiate an analyzer and feed it some text
    final String defaultText = "So I went to yoga last night for the first time in toooo long. The class was held at Grace Cathedral and it was amazing! I recommend it for those that live near SF: http://labyrinthyoga.com/ I'm even going to do my taxes today, that's how stress-free I woke up this morning ;)";
    final String text = (argv.length > 0 ? argv[0] : defaultText);
    // a reader on the text:
    StringReader reader = new StringReader(text);
    SocialAnalyzer analyzer = new SocialAnalyzer();
    TokenStream ts = analyzer.tokenStream("", reader);
    // capture the term attribute
    CharTermAttribute termAtt = ts.addAttribute(CharTermAttribute.class);
    PositionIncrementAttribute posAtt = ts.addAttribute(PositionIncrementAttribute.class);
    OffsetAttribute offAtt = ts.addAttribute(OffsetAttribute.class);
    try {
      while (ts.incrementToken()) {
        System.out.println("Got token '"+ termAtt.toString() + "' with pos inc " + posAtt.getPositionIncrement()
                            + " and offset [" + offAtt.startOffset() + ", " + offAtt.endOffset() + "]");
      }
    } catch (IOException e) {} // using a string reader. shouldn't happen.
  }
}