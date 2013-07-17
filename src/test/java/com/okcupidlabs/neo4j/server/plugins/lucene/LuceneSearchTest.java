package com.okcupidlabs.neo4j.server.plugins.lucene;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.*;

import org.codehaus.jackson.map.ObjectMapper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.index.Index;
import org.neo4j.kernel.impl.transaction.xaframework.ForceMode;
import org.neo4j.server.ServerTestUtils;
import org.neo4j.server.database.Database;
import org.neo4j.test.server.EntityOutputFormat;
import org.neo4j.server.rest.domain.GraphDbHelper;
import org.neo4j.server.rest.web.DatabaseActions;
import org.neo4j.server.rest.paging.FakeClock;
import org.neo4j.server.rest.paging.LeaseManager;
import org.neo4j.server.rest.repr.formats.JsonFormat;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import java.net.URI;
import java.net.URISyntaxException;

import java.io.IOException;
import java.util.Map;
import java.util.HashMap;

import static org.junit.Assert.assertEquals;

import java.util.logging.*;

public class LuceneSearchTest {

    private static final String BASE_URI = "http://neo4j.org/";
    private LuceneSearch service;
    private ObjectMapper objectMapper = new ObjectMapper();
    private static Database database;
    private static GraphDbHelper helper;
    private static EntityOutputFormat output;
    private static LeaseManager leaseManager;
    private static final ForceMode FORCE = ForceMode.forced;
    
    private final Logger log = Logger.getLogger(LuceneSearchTest.class.getName());

    @Before
    public void setUp() {
        database = new Database( ServerTestUtils.EPHEMERAL_GRAPH_DATABASE_FACTORY, null );
        helper = new GraphDbHelper( database );
        output = new EntityOutputFormat( new JsonFormat(), URI.create( BASE_URI ), null );
        leaseManager = new LeaseManager( new FakeClock() );
        service = new LuceneSearch( uriInfo(), new JsonFormat(), output,
                new DatabaseActions(database, leaseManager, ForceMode.forced, true), database.getGraph());
        LuceneSearchTestFixtures.populateDb(database.getGraph());
    }

    private static UriInfo uriInfo()
    {
        UriInfo mockUriInfo = mock( UriInfo.class );
        try
        {
            when( mockUriInfo.getBaseUri() ).thenReturn( new URI( BASE_URI ) );
        }
        catch ( URISyntaxException e )
        {
            throw new RuntimeException( e );
        }

        return mockUriInfo;
    }

    @Test
    public void shouldErrorIfBadIndex() {
        final Response response = service.search(FORCE, LuceneSearchTestFixtures.BAD_INDEX_FIXTURE);
        assertEquals(400, response.getStatus());
    }

    @Test
    public void shouldErrorIfMissingParameters() {
        final Response response = service.search(FORCE, "{\"min_score\": \"omfg\"}");
        assertEquals(400, response.getStatus());
    }

    // the search performs no graph manipulations, so no side effects to check
    // but we should make sure nothing bonks out here.
    // checking returned nodes can wait for the functional test.
    @Test
    public void shouldMatchNoNodes() {
        final Response response = service.search(FORCE, LuceneSearchTestFixtures.NO_RESULTS_FIXTURE);
        assertEquals(200, response.getStatus());
    }

    @Test
    public void shouldMatchSomeNodes() {
        final Response response = service.search(FORCE, LuceneSearchTestFixtures.SIM_PRESIDENT_OBAMA_FIXTURE);
        assertEquals(200, response.getStatus());
    }

    @Test
    public void dismax() {
        final Response response = service.search(FORCE, LuceneSearchTestFixtures.DISMAX_OBAMA_ROMNEY_FIXTURE);
        Object body = response.getEntity();
        assertEquals(200, response.getStatus());
    }
    
    @Test
    public void numeric() {
      final Response response = service.search(FORCE, LuceneSearchTestFixtures.NUM_RANGE_SEARCH_FIXTURE);
      Object body = response.getEntity();
      assertEquals(200, response.getStatus());
    }

    @After
    public void tearDown() throws Exception {
        try {
            database.shutdown();
        } catch (Throwable e) {
            // I don't care.
        }
    }

    public GraphDatabaseService graphdb() {
        return database.getGraph();
    }
}