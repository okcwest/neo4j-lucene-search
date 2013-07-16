package com.okcupidlabs.creeper.lucene.analysis;

import java.io.Reader;
import java.io.StringReader;
import java.io.IOException;
import java.util.HashSet;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.util.Version;

import java.util.logging.Logger;

public class NoArgumentStandardAnalyzer extends Analyzer {
  private final Analyzer standardAnalyzer;
  
  public NoArgumentStandardAnalyzer() {
    // instantiate a 3.5.0 analyzer with no stopwords
    standardAnalyzer = new StandardAnalyzer(Version.LUCENE_35, new HashSet<String>());
  }
  
  @Override
  public TokenStream tokenStream(String fieldName, Reader reader) {
    return standardAnalyzer.tokenStream(fieldName, reader);
  }
  
}