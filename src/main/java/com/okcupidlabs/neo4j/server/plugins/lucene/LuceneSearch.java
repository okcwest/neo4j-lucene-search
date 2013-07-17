package com.okcupidlabs.neo4j.server.plugins.lucene;

import java.util.Map;
import java.util.List;
import java.util.HashMap;
import java.util.ArrayList;
import java.net.URI;
import java.io.IOException;
import java.io.StringReader;

import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import javax.ws.rs.core.MediaType;

import org.neo4j.graphdb.*;
import org.neo4j.graphdb.index.IndexHits;
import org.neo4j.graphdb.index.Index;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.impl.transaction.xaframework.ForceMode;
import org.neo4j.server.rest.domain.PropertySettingStrategy;
import org.neo4j.server.rest.repr.*;
import org.neo4j.server.rest.web.DatabaseActions;
import org.neo4j.server.rest.web.PropertyValueException;
import org.neo4j.index.lucene.ValueContext;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.WhitespaceAnalyzer;
import org.apache.lucene.search.Query;

import org.apache.lucene.spatial.DistanceUtils;

import java.util.logging.*;


/**
 * Class containing JAX-RS endpoints for constructing complex lucene queries
 */
@Path("/")
public class LuceneSearch {

    private static final String[] REQUIRED_SEARCH_PARAMETERS = {"query_spec", "index_name"};
    private static final String[] REQUIRED_GEO_INDEX_PARAMETERS = {"index_name", "node_id", "lat", "lon"};
    private static final String[] REQUIRED_NUM_INDEX_PARAMETERS = {"index_name", "node_id", "index_key", "index_value"};
    private static final Class defaultAnalyzerClass = WhitespaceAnalyzer.class; // don't instantiate
    
    private final Logger log = Logger.getLogger(LuceneSearch.class.getName());


    private final UriInfo uriInfo;
    private final InputFormat input;
    private final OutputFormat output;
    private final DatabaseActions actions;
    private final GraphDatabaseService service;
    private final PropertySettingStrategy propertySetter;

    public LuceneSearch(@Context UriInfo uriInfo, @Context InputFormat input,
                              @Context OutputFormat output, @Context DatabaseActions actions,
                              @Context GraphDatabaseService service)
    {
        this.uriInfo = uriInfo;
        this.input = input;
        this.output = output;
        this.actions = actions;
        this.service = service;
        // NOTE: This is ugly as hell.  I don't want to depend on this cast but I do want
        // the PropertySettingStrategy instead of re-implementing that functionality.
        // WHATCHAGONNADO.
        this.propertySetter = new PropertySettingStrategy((GraphDatabaseAPI)service);

    }

    /**
     * Use index_value as a list of search terms to find in index_name:index_key. Use the same analyzer as index_name.
     *
     * @param force Force mode for transaction, normally used internally.
     * @param body JSON encoded parameters.
     *             Required:
     *             - index_name: Name of index to use for lookup
     *             - index_key: Index key to utilize for lookup
     *             - index_value: Index value to utilize for lookup.  Should be unique per index/key.
     *             - properties: Map of node properties to insert/merge
     *
     * @return JSON representation of node. (See: http://docs.neo4j.org/chunked/milestone/rest-api-node-properties.html)
     */
    @POST
    @Path("/search")
    public Response search(
                final @HeaderParam("Transaction") ForceMode force,
                final String body)
    {
        final PropertyMap<String, Object> properties;
        try {
            log.fine("Reading properties map from " + body);
            properties = new PropertyMap<String, Object>(input.readMap(body));
        } catch (BadInputException e) {
            log.warning("Broken input! Failed to decode " + body + ": " + e.getMessage());
            return output.badRequest(e);
        }

        if(!ensureRequiredParameters(properties, REQUIRED_SEARCH_PARAMETERS)) {
            return missingParameters(properties, REQUIRED_SEARCH_PARAMETERS);
        }

        String indexName = null;
        PropertyMap<String, Object> querySpec = null;
        try {
          indexName = (String)properties.get("index_name");
          querySpec = new PropertyMap((HashMap)properties.get("query_spec"));
        } catch (ClassCastException cce) {
          return output.badRequest(cce);
        }

        // optionally trim off low-quality hits
        // index query will need a float
        float minScore = 0;
        // depending on how this is called, though, we may have a double or int in the properties
        try {
          minScore = properties.getFloat("min_score");
        } catch (IllegalArgumentException iae) {
          log.warning("Ignoring illegal value for min_score: "+iae.getMessage());
        }
        
        //optionally use geo constraints.
        double lat = 200, lon = 200, dist = -1; // init to nonsense values

        try {
          lat = properties.getDouble("lat");
          if (lat > 90 || lat < -90)
            throw new IllegalArgumentException("Latitude must be in the range 0 +- 90, but was "+lat);
          // just warn; this arg is optional.
        } catch (IllegalArgumentException iae) {
          log.warning("Ignoring illegal value for latitude: "+iae.getMessage());
          lat = 200;
        }
        
        try {
          lon = properties.getDouble("lon");
          if (lon > 180 || lon < -180)
            throw new IllegalArgumentException("Longitude must be in the range 0 +- 180, but was "+lon);
          // just warn; this arg is optional.
        } catch (IllegalArgumentException iae) {
          log.warning("Ignoring illegal value for longitude: "+iae.getMessage());
          lon = 200;
        }
        
        try {
          dist = properties.getDouble("dist");
          if (dist <= 0)
            throw new IllegalArgumentException("Distance must be a positive value in miles, but was "+dist);
          // just warn; this arg is optional.
        } catch (IllegalArgumentException iae) {
          log.warning("Ignoring illegal value for distance: "+iae.getMessage());
          dist = -1;
        }

        // can't search an absent index
        if (!this.service.index().existsForNodes(indexName)) {
            return output.badRequest(
                    new IllegalArgumentException("Index with index_name: " + indexName + " does not exist."));
        }

        List<ScoredNode> searchResult = indexQuery(indexName, querySpec, minScore, lat, lon, dist);
        
        // build up a representation to be returned (there's got to be a better way!)
        List<ScoredNodeRepresentation> reprList = new ArrayList<ScoredNodeRepresentation>();
        for (ScoredNode sn : searchResult) {
          ScoredNodeRepresentation repr 
            = new ScoredNodeRepresentation(new NodeRepresentation(sn.getNode()), 
                                                                  sn.getScore());
          reprList.add(repr);
        }
        ListRepresentation reprListRepr = new ListRepresentation("org.neo4j.server.rest.repr.ScoredNodeRepresentation", reprList);

        return output.ok(reprListRepr);
    }

    @POST
    @Path("/index/numeric")
    public Response numericIndex(
                final @HeaderParam("Transaction") ForceMode force,
                final String body)
    {
        final PropertyMap<String, Object> properties;
        try {
            log.fine("Reading properties map from " + body);
            properties = new PropertyMap<String, Object>(input.readMap(body));
        } catch (BadInputException e) {
            log.warning("Broken input! Failed to decode " + body + ": " + e.getMessage());
            return output.badRequest(e);
        }

        if(!ensureRequiredParameters(properties, REQUIRED_NUM_INDEX_PARAMETERS)) {
            return missingParameters(properties, REQUIRED_NUM_INDEX_PARAMETERS);
        }
        
        // need an index_name, node, and coordinates.
        String indexName = null;
        long nodeId = 0;
        String indexKey = null;
        double indexValue = 0;
        try {
          indexName = (String)properties.get("index_name");
          indexKey = (String)properties.get("index_key");
          indexValue = properties.getDouble("index_value");
          nodeId = properties.getInt("node_id");
        } catch (ClassCastException cce) {
          return output.badRequest(cce);
        }
        
        // get the named index
        // INDEX MUST EXIST.
        if (!this.service.index().existsForNodes(indexName)) {
            return output.badRequest(
                    new IllegalArgumentException("Index with index_name: " + indexName + " does not exist."));
        }
        Index<Node> index = this.service.index().forNodes(indexName);
        
        Node node = null;
        try {
          node = numericIndex(this.service, index, nodeId, indexKey, indexValue);
        } catch (NotFoundException e) {
          return output.badRequest(e);
        }
        return output.ok(new NodeRepresentation(node));
    }
    
    /** Index a numeric value so that it can be searched by range.
    @param db  A connection to the db where we'll index this
    @param index The index to use
    @param nodeId  The id of the node we want to index
    @param key The index field where we'll index the node
    @param value the value to index
    @return The indexed node */
    public static Node numericIndex(GraphDatabaseService db, Index<Node> index, long nodeId, String key, double value) {
      Node node = db.getNodeById(nodeId);
      ValueContext vc = ValueContext.numeric(value);
      Transaction tx = db.beginTx();
      try {
        index.add(node, key, vc);
        tx.success();
      } finally {
        tx.finish();
      }
      return node;
    }
    
    @POST
    @Path("/index/geo")
    public Response geoIndex(
                final @HeaderParam("Transaction") ForceMode force,
                final String body)
    {
        final PropertyMap<String, Object> properties;
        try {
            log.fine("Reading properties map from " + body);
            properties = new PropertyMap<String, Object>(input.readMap(body));
        } catch (BadInputException e) {
            log.warning("Broken input! Failed to decode " + body + ": " + e.getMessage());
            return output.badRequest(e);
        }

        if(!ensureRequiredParameters(properties, REQUIRED_GEO_INDEX_PARAMETERS)) {
            return missingParameters(properties, REQUIRED_GEO_INDEX_PARAMETERS);
        }
        
        // need an index_name, node, and coordinates.
        String indexName = null;
        long nodeId = 0;
        double lat = 200, lon = 200;
        try {
          indexName = (String)properties.get("index_name");
          lat = properties.getDouble("lat");
          lon = properties.getDouble("lon");
          nodeId = properties.getInt("node_id");
        } catch (ClassCastException cce) {
          return output.badRequest(cce);
        }
        
        // get the named index
        // INDEX MUST EXIST.
        if (!this.service.index().existsForNodes(indexName)) {
            return output.badRequest(
                    new IllegalArgumentException("Index with index_name: " + indexName + " does not exist."));
        }
        Index<Node> index = this.service.index().forNodes(indexName);
        
        Node node = null;
        try {
          node = geoIndex(this.service, index, nodeId, lat, lon);
        } catch (NotFoundException e) {
          return output.badRequest(e);
        }
        return output.ok(new NodeRepresentation(node));
    }
    
    // make this available for ad hoc indexing
    public static Node geoIndex(GraphDatabaseService db, Index<Node> index, long nodeId, double lat, double lon) throws NotFoundException {
      // retrieve the node we want to index
      Node node = db.getNodeById(nodeId);

      // index the business. we can't actually use lucene spatial, 
      // because neo4j doesn't expose the index at a low enough level.
      ValueContext latValue = ValueContext.numeric(lat);
      ValueContext lonValue = ValueContext.numeric(lon);
      // fooling with the nodespace needs to be atomic
      Transaction tx = db.beginTx();
      try {
        index.add(node, QueryBuilder.LAT_KEY, latValue);
        index.add(node, QueryBuilder.LON_KEY, lonValue);
        tx.success();
      } finally {
        tx.finish();
      }

      // nothin' broke.
      return node;
    }
    /**
     * Search the named index, building a query as specified.
     * @param querySpec     a JSON representation of a query, which may be nested.
     * @param minScore  minimum similarity score to accept. 0 for no limit.
     * @return A list of nodes that match the query
     */
    private List<ScoredNode> indexQuery(
            final String indexName,
            final PropertyMap<String, Object> querySpec,
            final float minScore,
            final double lat,
            final double lon,
            final double dist)
    {
        // make sure the index contains the desired key
        // this call will create an index, if none was there, so the caller 
        // is responsible for caring about whether it existed beforehand.
        Index<Node> index = this.service.index().forNodes(indexName);
        Analyzer analyzer = getIndexAnalyzer(indexName);
        // build query AFTER we get the index above, to ensure it has been created if it was absent.
        Query query = QueryBuilder.buildQuery(analyzer, querySpec);
        
        // we'll need to make sure this doesn't have the query in it due to lucene weirdies.
        IndexHits<Node> queryResults = index.query(query);
        List<ScoredNode> resultsList = new ArrayList<ScoredNode>();
        for (Node n : queryResults) {
          // pack the similarity score into the node.
          float score = queryResults.currentScore();
          if (score < minScore) {
            log.info("Dropping low-scoring node: score " + score + " < min score " + minScore);
            continue;
          } 
          log.fine("Score " + score + " for node " + n + " passes threshold " + minScore);
          // are we checking distances?
          if (lat != 200 && lon != 200 && dist != -1) {
            // see if this node has a lat/lon associated
            try {
              double nLat = PropertyMap.getDoubleFromObject(n.getProperty("lat"));
              double nLon = PropertyMap.getDoubleFromObject(n.getProperty("lon"));
              if (nLat < -90 || nLat > 90) {
                log.warning("Node has a bogus value of "+nLat+" for latitude");
              } else if (nLon < -180 || nLon > 180) {
                log.warning("Node has a bogus value of "+nLon+" for longitude");
              } else if (!inRadius (lat, lon, nLat, nLon, dist)) {
                log.info("Dropping node that is too far away.");
                continue;
              }
            } catch (IllegalArgumentException iae) {
              // do nothing.
            }
          }
          log.fine("Adding node " + n + " with score " + score);
          resultsList.add(new ScoredNode(n, score));
        }
        queryResults.close(); // must release the search result's resources.
        return resultsList;
    }
    
    private boolean inRadius(double lat1, double lon1, double lat2, double lon2, double maxDist) {
      double rLat1 = DistanceUtils.DEGREES_TO_RADIANS * lat1;
      double rLon1 = DistanceUtils.DEGREES_TO_RADIANS * lon1;
      double rLat2 = DistanceUtils.DEGREES_TO_RADIANS * lat2;
      double rLon2 = DistanceUtils.DEGREES_TO_RADIANS * lon2;
      double dist = DistanceUtils.haversine(rLat1, rLon1, rLat2, rLon2, DistanceUtils.EARTH_MEAN_RADIUS_MI);
      log.fine("From ("+lat1+","+lon1+") to ("+lat2+","+lon2+") is "+dist+"mi");
      return dist <= maxDist;
    }
    
    private Analyzer getIndexAnalyzer(String indexName) {
      Index<Node> index = this.service.index().forNodes(indexName);
      // we'll need an analyser of the same kind as the index. instantiation is hinky.
      // what helps is that all neo4j analyzers currently must be constructed with no args.
      Map<String, String> indexConfig = this.service.index().getConfiguration(index);
      final String indexAnalyzerClassname = indexConfig.get("analyzer");
      Analyzer analyzer = null;
      if (indexAnalyzerClassname != null) {
        // try to instantiate it from the index config
        try {
          Class analyzerClass = Class.forName(indexAnalyzerClassname);
          analyzer = (Analyzer) analyzerClass.getConstructor().newInstance();
        } catch (Exception e) {
          // fail over to the default
          log.warning("Failed to instantiate index analyzer type " + indexAnalyzerClassname 
            + ":" + e.getMessage() + ". Using WhitespaceAnalyzer instead.");
          analyzer = new WhitespaceAnalyzer();
        }
      }
      return analyzer;
    }
    
    private class ScoredNode {
      private Node node;
      private float score;
      
      public Node getNode() {
        return node;
      }
      
      public float getScore() {
        return score;
      }

      ScoredNode(Node n, float s) {
        this.node = n;
        this.score = s;
      }
    }
    
    /**
     * Validates that required parameters are supplied in property map
     * @param properties Map containing supplied parameters to endpoint
     * @return False if any required keys are missing
     */
    public static boolean ensureRequiredParameters(Map<String, Object> properties, String ... requiredKeys)
    {
        for (String key : requiredKeys) {
            if (properties.get(key) == null) {
                return false;
            }
        }
        return true;
    }

    /**
     * Helper method for generating response when required parameters are missing
     * @param properties Map containing supplied parameters to endpoint
     * @return Response representing failed conditions on endpoint
     */
    private Response missingParameters(Map<String, Object> properties, String ... requiredKeys)
    {
        String[] receivedParams = properties.keySet().toArray(new String[0]);

        return Response.status( 400 )
                .type(MediaType.TEXT_PLAIN)
                .entity("Required parameters: " + implode(requiredKeys) + "\n"
                      + "Received parameters: " + implode(receivedParams))
                .build();
    }

    private static String implode(String[] receivedParams) {
        String receivedParamString = "";
        if (receivedParams.length > 0) {
            StringBuilder sb = new StringBuilder();
            sb.append(receivedParams[0]);

            for (int i = 1; i < receivedParams.length; i++) {
                sb.append(", ");
                sb.append(receivedParams[i]);
            }
            receivedParamString = sb.toString();
        }
        return receivedParamString;
    }

    private Response badJsonFormat(String body) {
        return Response.status( 400 )
                .type( MediaType.TEXT_PLAIN )
                .entity( "Invalid JSON array in POST body: " + body )
                .build();
    }

}
