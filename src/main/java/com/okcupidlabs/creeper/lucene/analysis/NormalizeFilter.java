package com.okcupidlabs.creeper.lucene.analysis;

import java.io.IOException;
import java.util.List;
import java.util.ArrayList;
import java.util.regex.Pattern;
import java.util.regex.Matcher;
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import java.util.logging.Logger;

// this adds lowercased and punctuation-stripped tokens, but preserves the raw tokens on the stream
public final class NormalizeFilter extends TokenFilter {
  
  private Logger log = Logger.getLogger(NormalizeFilter.class.getName());

  private Pattern nonWord = Pattern.compile("^[^\\d\\w]*$"); // things we're going to leave alone, like ":)" and "--"
  private Pattern openParen = Pattern.compile("^\\("); // led by a parenthesis
  private Pattern closeParen = Pattern.compile("\\)$"); // trailed by a parenthesis
  private Pattern terminalPunctuation = Pattern.compile("[\\.\\!\\?]+$"); // any grouping of sentence terminators
  private Pattern conjunctivePunctuation = Pattern.compile("[,;:]$"); // these are not normally grouped, and occur at the end
  private Pattern capitalized = Pattern.compile("^[A-Z]"); // starts with a capital letter

  // attributes we are going to capture and alter from the underlying stream
  private CharTermAttribute term;
  private PositionIncrementAttribute posInc;
  private OffsetAttribute offAtt;

  // a buffer to consume expanded tokens from before incrementing the input stream.
  private List<PartialTerm> termBuffer = new ArrayList<PartialTerm>(); 
  
  public NormalizeFilter(TokenStream input) {
    super(input);
    term = input.addAttribute(CharTermAttribute.class);
    posInc = input.addAttribute(PositionIncrementAttribute.class);
  }

  public boolean incrementToken() throws IOException {
    // should we consume something off the buffer first?
    if (termBuffer.size() > 0) {
      log.fine("Consuming from buffer");
      // if reading from the buffer, this means we are doing an expansion.
      // we need to set the position attribute to 0, coz we're consuming things that duplicate a single position in the source text
      posInc.setPositionIncrement(0);
      PartialTerm next = termBuffer.remove(0); // pop a term
      // offset and term come from the termbuffer object.
      offAtt.setOffset(next.startOffset, next.endOffset);
      term.setEmpty();
      term.append(next.term);
      return true; // don't bump the token underneath.
    }
    // does the current term need to be skipped?
    String termString = term.toString();
    if (nonWord.matcher(termString).find()) {
      log.fine("Found a nonword: " + termString);
      // just bump to the next token from the source
      // TODO: add a nonWord attribute.
      return input.incrementToken();
    }
    // we are allowed to manipulate this. skip parens for a moment.
    // is there trailing punctuation to split off?
    Matcher terminalPunctuationMatcher = terminalPunctuation.matcher(termString);
    if (terminalPunctuationMatcher.find()) {
      log.fine("Terminal punctuation found: " + terminalPunctuationMatcher.group());
      
      return input.incrementToken();
    }
    
    return input.incrementToken();
  }
  
  private class PartialTerm {
    String term;
    int startOffset;
    int endOffset;
    
    PartialTerm(String _term, int _start, int _end) {
      term = _term;
      startOffset = _start;
      endOffset = _end;
    }
  }
}