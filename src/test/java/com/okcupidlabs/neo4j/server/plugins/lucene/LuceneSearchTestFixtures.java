package com.okcupidlabs.neo4j.server.plugins.lucene;

import java.util.Map;
import java.util.HashMap;

import org.neo4j.graphdb.*;
import org.neo4j.graphdb.index.Index;

import java.util.logging.*;
import java.util.Calendar;

public class LuceneSearchTestFixtures {

    public static final String DEFAULT_ANALYZER = "org.apache.lucene.analysis.WhitespaceAnalyzer";
    //public static final String DEFAULT_ANALYZER = "okcupidlabs.creeper.lucene.analysis.NoArgumentStandardAnalyzer";
    public static final String INDEX_NAME = "content";

    private static PropertyMap<String, Object> easter = new PropertyMap<String, Object>();
    private static PropertyMap<String, Object> indDay = new PropertyMap<String, Object>();
    private static PropertyMap<String, Object> electDay = new PropertyMap<String, Object>();
    private static PropertyMap<String, Object> christmas = new PropertyMap<String, Object>();
    
    static {
      easter.put("timestamp", getTimestamp(2013, 3, 31, 0, 0));
      indDay.put("timestamp", getTimestamp(2013, 7, 4, 0, 0));
      electDay.put("timestamp", getTimestamp(2013, 11, 5, 0, 0));
      christmas.put("timestamp", getTimestamp(2013, 12, 25, 0, 0));
    }
  
    public static final String BAD_INDEX_FIXTURE = "{" +
            "\"index_name\": \"absent\"," +
            "\"query_spec\": {" +
              "\"type\": \"SIM\"," +
              "\"index_key\": \"text\"," +
              "\"query\": \"doesn't match anything.\"" +
              "}" +
            "}";

    public static final String NO_RESULTS_FIXTURE = "{" +
            "\"index_name\": \"" + INDEX_NAME + "\"," +
            "\"query_spec\": {" +
              "\"type\": \"SIM\"," +
              "\"index_key\": \"text\"," +
              "\"query\": \"doesn't match anything.\"" +
              "}" +
            "}";

    public static final String SIM_PRESIDENT_OBAMA_FIXTURE = "{" +
            "\"index_name\": \"" + INDEX_NAME + "\"," +
            "\"query_spec\": {" +
              "\"type\": \"SIM\"," +
              "\"index_key\": \"text\"," +
              "\"query\": \"President Obama\"" + // match case coz we are testing with whitespace analyzer
              "}" +
            "}";

    public static final String PRESIDENT_OBAMA_MIN_SCORE_FIXTURE = "{" +
            "\"index_name\": \"" + INDEX_NAME + "\"," +
            "\"min_score\": 0.1," +
            "\"query_spec\": {" +
              "\"type\": \"SIM\"," +
              "\"index_key\": \"text\"," +
              "\"query\": \"President Obama\"" + // match case coz we are testing with whitespace analyzer
              "}" +
            "}";
    
    public static final String DISMAX_OBAMA_ROMNEY_FIXTURE = "{" +
            "\"index_name\": \"" + INDEX_NAME + "\"," +
            "\"min_score\": 0.1," +
            "\"query_spec\": {" +
              "\"type\": \"DISMAX\"," +
              "\"subqueries\": [" +
                "{\"type\": \"SIM\"," +
                  "\"index_key\": \"text\"," +
                  "\"query\": \"Obama\"" + // match case coz we are testing with whitespace analyzer
                  "}," +
                "{\"type\": \"SIM\"," +
                  "\"index_key\": \"text\"," +
                  "\"query\": \"Romney\"" + // match case coz we are testing with whitespace analyzer
                  "}" +
                "]" +
              "}" +
            "}";
            
    public static final String BOOLEAN_BAD_OCCURS_FIXTURE = "{" +
            "\"index_name\": \"" + INDEX_NAME + "\"," +
            "\"min_score\": 0.1," +
            "\"query_spec\": {" +
              "\"type\": \"BOOL\"," +
              "\"clauses\": [" +
                "{\"query_spec\": {" +
                    "\"type\":\"TERM\"," +
                    "\"index_key\": \"text\"," +
                    "\"query\": \"President Obama\"" + // match case coz we are testing with whitespace analyzer
                    "}," +
                  "\"occurs\": \"CAN\"" +
                  "}" +
                "]" +
              "}" +
            "}";

    public static final String BOOLEAN_FIXTURE = "{" +
            "\"index_name\": \"" + INDEX_NAME + "\"," +
            "\"min_score\": 0.1," +
            "\"query_spec\": {" +
              "\"type\": \"BOOL\"," +
              "\"clauses\": [" +
                "{\"query_spec\": {" +
                    "\"type\":\"TERM\"," +
                    "\"boost\": 1.5," +
                    "\"index_key\": \"text\"," +
                    "\"query\": \"President\"" + // match case coz we are testing with whitespace analyzer
                    "}," +
                  "\"occurs\": \"MUST\"" +
                  "}," +
                "{\"query_spec\": {" +
                    "\"type\":\"TERM\"," +
                    "\"index_key\": \"text\"," +
                    "\"query\": \"Obama\"" + // match case coz we are testing with whitespace analyzer
                    "}," +
                  "\"occurs\": \"SHOULD\"" +
                  "}" +
                "]" +
              "}" +
            "}";

    public static final String PHRASE_FIXTURE = "{" +
            "\"index_name\": \"" + INDEX_NAME + "\"," +
            "\"min_score\": 0.1," +
            "\"query_spec\": {" +
              "\"type\":\"PHRASE\"," +
              "\"boost\": 1.5," +
              "\"index_key\": \"text\"," +
              "\"query\": \"Barack Obama\"" + // match case coz we are testing with whitespace analyzer
              "}" +
            "}";

    public static final String PHRASE_SLOP_FIXTURE = "{" +
            "\"index_name\": \"" + INDEX_NAME + "\"," +
            "\"min_score\": 0.1," +
            "\"query_spec\": {" +
              "\"type\":\"PHRASE\"," +
              "\"boost\": 1.5," +
              "\"index_key\": \"text\"," +
              "\"query\": \"President Obama\"," + // match case coz we are testing with whitespace analyzer
              "\"slop\": 1" +
              "}" +
            "}";

    public static final String GEO_CONSTRAINED_FIXTURE = "{" +
            "\"index_name\": \"" + INDEX_NAME + "\"," +
            "\"min_score\": 0.1," +
            "\"lat\": 40.7142," + // new york
            "\"lon\": -74.0064," +
            "\"dist\": 300," +
            "\"query_spec\": {" +
              "\"type\":\"TERM\"," +
              "\"index_key\": \"text\"," +
              "\"query\": \"Obama\"" + // match case coz we are testing with whitespace analyzer
              "}" +
            "}";

    public static final String GEO_SEARCH_FIXTURE = "{" +
            "\"index_name\": \"" + INDEX_NAME + "\"," +
            "\"min_score\": 0.1," +
            "\"query_spec\": {" +
              "\"type\":\"GEO\"," +
              "\"lat\": 40.7142," + // new york
              "\"lon\": -74.0064," +
              "\"dist\": 300" +
              "}" +
            "}";

    public static final String NUM_RANGE_SEARCH_FIXTURE = "{" +
            "\"index_name\": \"" + INDEX_NAME + "\"," +
            "\"min_score\": 0.1," +
            "\"query_spec\": {" +
              "\"type\":\"NUMRANGE\"," +
              "\"index_key\": \"timestamp\"," + // new york
              "\"range\": \"("+easter.get("timestamp")+","+electDay.get("timestamp")+"]\"" +
              "}" +
            "}";


    private static final Logger log = Logger.getLogger(LuceneSearchTestFixtures.class.getName());
    
    public static long getTimestamp(int year, int month, int day, int hour, int minute) {
      Calendar cal = Calendar.getInstance();
      cal.set(year, month, day, hour, minute, 0);
      return cal.getTimeInMillis();
    }
    
    // functions for populating the graph database with test data
    public static void populateDb(GraphDatabaseService db) {
        // set up some locations to use
        PropertyMap<String, Object> honoluluLatLong = new PropertyMap<String, Object>();
        honoluluLatLong.put("lat", new Double(21.3069));
        honoluluLatLong.put("lon", new Double(-157.8583));
      
        PropertyMap<String, Object> washingtonLatLong = new PropertyMap<String, Object>();
        washingtonLatLong.put("lat", new Double(38.89));
        washingtonLatLong.put("lon", new Double(-77.03));
      
        PropertyMap<String, Object> bostonLatLong = new PropertyMap<String, Object>();
        bostonLatLong.put("lat", new Double(42.3583));
        bostonLatLong.put("lon", new Double(-71.0603));
              
        // need to index several text strings which have words in common
        Transaction tx = db.beginTx();
        try
        {
          Index<Node> index = initializeCleanIndex(db, DEFAULT_ANALYZER, INDEX_NAME);
          Node baseballNode = createIndexedNode(db, index, "Baseball was once considered America's national pastime.", easter);
          Node presidentNode = createIndexedNode(db, index, "America elects a new President every four years, in November.", electDay);
          Node obamaNode = createIndexedNode(db, index, "Barack Obama was born in Honolulu, Hawaii.", honoluluLatLong);
          Node romneyNode = createIndexedNode(db, index, "Mitt Romney ran for President of the United States of America in 2012", bostonLatLong);
          Node obamaPresidentNode = createIndexedNode(db, index, "President Barack Obama gave the State of the Union address on Tuesday", washingtonLatLong);
          Node obamaBaseballNode = createIndexedNode(db, index, "President Obama threw out the first pitch of the 2010 baseball season.", washingtonLatLong);
          Node mittBaseballNode = createIndexedNode(db, index, "A baseball mitt is worn on a pitcher's off hand.", indDay);
          Node romneyPresidentNode = createIndexedNode(db, index, "Romney's campaign for President suffered from his lack of a relatable image and his evident barking insanity.", christmas);
          tx.success();
        }
        finally
        {
            tx.finish();
        }
    }

    // return the created index
    private static Index<Node> initializeCleanIndex(GraphDatabaseService db, String analyzerClassname, String indexName) {
      // if an index by this name was there already, remove it.
      if (db.index().existsForNodes(indexName)) {
        db.index().forNodes(indexName).delete();
      }
      Map<String, String> config = new HashMap();
      config.put("analyzer", analyzerClassname);
      Index<Node> index = null;
      try {
        index = db.index().forNodes(indexName, config);
      } catch (IllegalArgumentException e) {
        log.warning("Index called " + indexName + " already exists and could not be removed.");
      }
      return index;
    }

    // takes an index which is thus guaranteed to exist
    private static Node createIndexedNode(GraphDatabaseService db, Index<Node> index, String text) {
      return createIndexedNode(db, index, text, null);
    }
    
    private static Node createIndexedNode(GraphDatabaseService db, Index<Node> index, String text, PropertyMap<String, Object> props) {
        Node node = db.createNode();
        node.setProperty("text", text);
        // set other properties from map
        if (props != null) {
          for (String key : props.keySet()) {
            node.setProperty(key, props.get(key));
          }
          // if there are lat/long, index coords
          if (props.containsKey("lat") && props.containsKey("lon")) {
            LuceneSearch.geoIndex(db, index, node.getId(), props.getDouble("lat"), props.getDouble("lon"));
          }
          // if there is timestamp data, index that numerically.
          if (props.containsKey("timestamp")) {
            LuceneSearch.numericIndex(db, index, node.getId(), "timestamp", props.getDouble("timestamp"));
          }
          
        }
        index.add(node, "text", text);
        return node;
    }

}
