package com.okcupidlabs.neo4j.server.plugins.lucene;

import java.util.Map;
import java.util.List;
import java.util.HashMap;
import java.util.ArrayList;
import java.net.URI;
import java.io.IOException;
import java.io.StringReader;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.NumericRangeQuery;
import org.apache.lucene.search.NumericRangeFilter;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BooleanFilter;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.DisjunctionMaxQuery;
import org.apache.lucene.search.FilteredQuery;
import org.apache.lucene.index.Term;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.search.similar.SimilarityQueries;

import org.apache.lucene.spatial.tier.LatLongDistanceFilter;
import org.apache.lucene.spatial.geometry.shape.LLRect;
import org.apache.lucene.spatial.geometry.FloatLatLng;

import java.util.regex.Pattern;
import java.util.regex.Matcher;
import java.util.logging.*;

public class QueryBuilder {
  public QueryBuilder() {}; // blank constructor

  private static final float DEFAULT_DISMAX_TIEBREAKER = 0.1f;
  public static final String LAT_KEY = "lat", LON_KEY = "lon"; // where to index the geo data
  private static final Logger log = Logger.getLogger(QueryBuilder.class.getName());
  private static Pattern numRangePattern = Pattern.compile("^([\\(\\[])(.*),(.*)([\\)\\]])$");
  
  /**
   * Recursively build up a query object as described by the given query spec.
   * Use the given analyzer as needed.
   */
  public static Query buildQuery(Analyzer analyzer, PropertyMap<String, Object> querySpec) throws IllegalArgumentException {
    // a valid query object has a type, and then some data.
    QueryType type = null;
    try {
      type = Enum.valueOf(QueryType.class, (String)querySpec.get("type"));
    } catch (NullPointerException npe) {
      throw new IllegalArgumentException("Query spec "+querySpec+" has no type");
    }
    // what kind of query is this?
    // can support term, geo, sim, or a nested dismax.
    // only sim and dismax for now.
    Query q = null;
    switch (type) {
      case DISMAX:
        // there will be a list of maps, which contain other query specs for building up the query.
        List<Map<String, Object>> subSpecs = (List<Map<String, Object>>)querySpec.get("subqueries");
        if (subSpecs == null || subSpecs.size() == 0) {
          throw new IllegalArgumentException("Dismax query must contain a list of valid subqueries");
        }
        float dismaxTieBreaker = DEFAULT_DISMAX_TIEBREAKER;
        try {
          dismaxTieBreaker = querySpec.getFloat("tiebreaker");
        } catch (IllegalArgumentException iae) {
          log.info("Using default dismax tiebreaker: " + iae.getMessage());
        }
        q = makeDismaxQuery(analyzer, subSpecs, dismaxTieBreaker);
        break;
      case BOOL:
        List<Map<String, Object>> clauses = (List<Map<String, Object>>)querySpec.get("clauses");
        if (clauses == null || clauses.size() == 0) {
          throw new IllegalArgumentException("Boolean query must contain a list of clauses.");
        }
        q = makeBooleanQuery(analyzer, clauses);
        break;
      case NUMRANGE:
        String key = (String)querySpec.get("index_key");
        String range = (String)querySpec.get("range");
        q = makeNumRangeQuery(key, range);
        break;
      case GEO:
        double lat = querySpec.getDouble("lat"); // these are required. they'll barf on a bad value and that's fine.
        double lon = querySpec.getDouble("lon");
        double dist = querySpec.getDouble("dist");
        q = makeGeoQuery(lat, lon, dist);
        break;
      case TERM:
        String numericKey = (String)querySpec.get("index_key");
        String queryString = (String)querySpec.get("query");
        if (numericKey == null || queryString == null) {
          throw new IllegalArgumentException("Trying to build a term query, but missing index key or query.");
        }
        q = new TermQuery(new Term(numericKey, queryString));
        break;
      case PHRASE:
        String phraseKey = (String)querySpec.get("index_key");
        String phraseString = (String)querySpec.get("query");
        if (phraseKey == null || phraseString == null) {
          throw new IllegalArgumentException("Trying to build a phrase query, but missing index key or query.");
        }
        int slop = 0;
        try {
          slop = querySpec.getInt("slop");
        } catch (IllegalArgumentException iae) {
          log.warning("Ignoring phrase slop: " + iae.getMessage());
        }
        try {
          q = makePhraseQuery(analyzer, phraseKey, phraseString, slop);
        } catch (IOException e) {
          throw new IllegalArgumentException("Failed creating phrase query: "+e.getMessage());
        }
        break;
      case SIM:
        // similarity query. should have keys for index key and query.
        String indexKey = (String)querySpec.get("index_key");
        String simQueryString = (String)querySpec.get("query");
        if (indexKey == null || simQueryString == null) {
          throw new IllegalArgumentException("Trying to build a similarity query, but missing index key or query.");
        }
        q = makeSimilarityQuery(analyzer, indexKey, simQueryString);
        break;
      default:
        throw new IllegalArgumentException("Unsupported query type: "+type.name());
    }
    // now that we have the query object, set the boost on it.
    float boost = 1.0f; // default to no boost
    try {
      boost = querySpec.getFloat("boost");
    } catch (IllegalArgumentException iae) {
      log.warning("Couldn't set boost on query of type " + type + ": " + iae.getMessage());
    }
    q.setBoost(boost);
    return q;
  }

  // some static methods for assembling queries.
  public static Query makeSimilarityQuery(Analyzer analyzer, String key, String query) {
    // set up a similarity query with the inbound object and analyzer, and no stop words
    Query similarityQuery;
    try {
      similarityQuery = SimilarityQueries.formSimilarQuery(query, analyzer, key, null);
    } catch (IOException e) {
      // per lucene docs, this can't actually happen. for argument's sake, log and make a junk term query instead.
      log.warning("Impossible exception encountered when forming the similarity query. Searching as term instead.");
      similarityQuery = new TermQuery(new Term(key, query));
    }
    return similarityQuery;
  }
  
  public static Query makePhraseQuery(Analyzer analyzer, String key, String query, int slop) throws IOException {
    List<Term> terms = extractTerms(analyzer, key, query);
    PhraseQuery q = new PhraseQuery();
    for (Term term : terms) {
      q.add(term);
    }
    q.setSlop(slop);
    return q;
  }
  
  public static Query makeDismaxQuery(Analyzer analyzer, List<Map<String, Object>> subSpecs, float tiebreaker) 
      throws IllegalArgumentException 
  {
    List<Query> subQueries = new ArrayList<Query>();
    for (Map<String, Object> subSpec : subSpecs) {
      subQueries.add(buildQuery(analyzer, new PropertyMap(subSpec)));
    }
    return new DisjunctionMaxQuery(subQueries, tiebreaker);
  }
  
  public static Query makeBooleanQuery(Analyzer analyzer, List<Map<String, Object>> clauses) throws IllegalArgumentException {
    BooleanQuery bQuery = new BooleanQuery();
    for (Map<String, Object> clause : clauses) {
      // should contain a query spec...
      Map<String, Object> subSpec = (Map<String, Object>)clause.get("query_spec");
      if (subSpec == null) {
        throw new IllegalArgumentException("Can't construct a boolean clause: missing query spec.");
      }
      Query subQuery = null;
      try {
        subQuery = buildQuery(analyzer, new PropertyMap(subSpec));
      } catch (IllegalArgumentException iae) {
        throw new IllegalArgumentException("Can't construct a boolean clause: bad query spec! " + iae.getMessage());
      }
      // also an OCCURS value.
      BooleanClause.Occur occurs = null;
      try {
          occurs = Enum.valueOf(BooleanClause.Occur.class, (String)clause.get("occurs"));
        } catch (NullPointerException npe) {
          throw new IllegalArgumentException("Clause "+clause+" has missing or bad occurs value. Must be MUST|MUST_NOT|SHOULD.");
      }
      bQuery.add(new BooleanClause(subQuery, occurs));
    }
    return bQuery;
  }

  public static Query makeNumRangeQuery(String numericKey, String range) 
      throws IllegalArgumentException 
  {
    // unpack the range string to a value
    Matcher rangeMatcher = numRangePattern.matcher(range);
    double minVal = 0, maxVal = 0;
    boolean minInc = false, maxInc = false;
    if (rangeMatcher.matches()) {
      try {
        minVal = Double.parseDouble(rangeMatcher.group(2));
        maxVal = Double.parseDouble(rangeMatcher.group(3));
      } catch (NumberFormatException nfe) {
        throw new IllegalArgumentException(nfe);
      }
      // numbers ok. get inclusivity
      minInc = (rangeMatcher.group(1).equals("["));
      maxInc = (rangeMatcher.group(4).equals("]"));
    } else {
      throw new IllegalArgumentException("Couldn't convert "+range+" to a range like [nnn,mmm)");
    }
    return NumericRangeQuery.newDoubleRange(numericKey, minVal, maxVal, minInc, maxInc);
  }
  
  public static Query makeGeoQuery(double lat, double lon, double dist) {
    // make a bounding box using a filtered query.
    // this is weirdly complicated, because distance query can only be implemented as a filter,
    // but distance math is expensive. so, in order, we 
    //   -- filter on latitude, which is always a continuous NumericRange. This performs a Query.
    //   -- filter that filter on longitude, which is sometimes two NumericRanges. Compose filters if needed. These perform Queries too.
    //   -- perform distance filter on the composition of these filters.
    //   -- apply all those filters to the results of a NumericRangeQuery for the lat range.
    //   -- this performs the lat query twice, effectively, but is worthwhile because it saves us lots of distance math.
    // we CAN use spatial utils here.
    double diam = 2*dist;
    // if the bounding box crosses the meridian, things will be weird.
    // LLRect does the right thing near the poles.
    LLRect bb = LLRect.createBox(new FloatLatLng(lat, lon), diam, diam);
    double lowerLat = bb.getLowerLeft().getLat();
    double upperLat = bb.getUpperRight().getLat();
    NumericRangeFilter latFilter = NumericRangeFilter.newDoubleRange(LAT_KEY, lowerLat, upperLat, true, true);
    // longitude query is where things get hinky at the opposite meridian.
    double leftLon = bb.getLowerLeft().getLng();
    double rightLon = bb.getUpperRight().getLng();
    BooleanFilter lonFilter = new BooleanFilter();
    if (leftLon > lon || rightLon < lon) {
      // box crosses the meridian. compose two queries.
      NumericRangeFilter leftLonFilter = NumericRangeFilter.newDoubleRange(LON_KEY, leftLon, 180d, true, true);
      NumericRangeFilter rightLonFilter = NumericRangeFilter.newDoubleRange(LON_KEY, -180d, rightLon, true, true);
      lonFilter.add(leftLonFilter, BooleanClause.Occur.SHOULD); // these get OR'd together.
      lonFilter.add(rightLonFilter, BooleanClause.Occur.SHOULD);
    } else {
      lonFilter.add(NumericRangeFilter.newDoubleRange(LON_KEY, leftLon, rightLon, true, true), BooleanClause.Occur.MUST);
    }
    // compose lat and lon filters
    BooleanFilter bbFilter = new BooleanFilter();
    bbFilter.add(latFilter, BooleanClause.Occur.MUST); // AND these together to get a bounding box.
    bbFilter.add(lonFilter, BooleanClause.Occur.MUST);
    // FINALLY, apply a radial distance filter on the result.
    LatLongDistanceFilter radialFilter = new LatLongDistanceFilter(bbFilter, lat, lon, dist, LAT_KEY, LON_KEY);
    // now turn this all back into a query. seed it with a lat query.
    NumericRangeQuery latQuery = NumericRangeQuery.newDoubleRange(LAT_KEY, lowerLat, upperLat, true, true);
    FilteredQuery radialQuery = new FilteredQuery(latQuery, radialFilter);
    return radialQuery;
  }
  
  // make a Term list for a Phrase query.
  private static List<Term> extractTerms(Analyzer analyzer, String fieldName, String doc) throws IOException {
    List<Term> terms = new ArrayList<Term>();
    StringReader reader = new StringReader(doc);
    TokenStream ts = analyzer.tokenStream(fieldName, reader);
    CharTermAttribute termAtt = ts.addAttribute(CharTermAttribute.class);
    try {
      while (ts.incrementToken()) {
        terms.add(new Term(fieldName, termAtt.toString()));
      }
    } catch (IOException ioe) {
      // fail with a more descriptive exception
      throw new IOException("Failed extracting terms from string '"+doc+"': "+ioe.getMessage());
    }
    return terms;
  }
  
}