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

/**
* Analyzers specified for a neo4j index must have a no-argument constructor. 
* This class wraps a StandardAnalyzer of version 3.5.0 with no stopwords
*/
public class NoArgumentStandardAnalyzer extends Analyzer {
  private final Analyzer standardAnalyzer;
  
  /**
  * Instantiates a StandardAnalyzer of version 3.5.0 with no stopwords
  */
  public NoArgumentStandardAnalyzer() {
    // instantiate a 3.5.0 analyzer with no stopwords
    standardAnalyzer = new StandardAnalyzer(Version.LUCENE_35, new HashSet<String>());
  }
  
  /**
  * Wraps the tokenStream of the wrapped StandardAnalyzer
  */
  @Override
  public TokenStream tokenStream(String fieldName, Reader reader) {
    return standardAnalyzer.tokenStream(fieldName, reader);
  }
  
}