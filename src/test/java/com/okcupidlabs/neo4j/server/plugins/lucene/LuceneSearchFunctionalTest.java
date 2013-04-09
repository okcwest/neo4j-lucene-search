package com.okcupidlabs.neo4j.server.plugins.lucene;

import java.io.IOException;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.List;

import com.sun.jersey.api.client.Client;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.type.TypeFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.index.Index;
import org.neo4j.server.NeoServer;
import org.neo4j.server.helpers.ServerBuilder;
import org.neo4j.server.rest.JaxRsResponse;
import org.neo4j.server.rest.RestRequest;


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.logging.*;

public class LuceneSearchFunctionalTest {

    public static final Client CLIENT = Client.create();
    public static final String MOUNT_POINT = "/ext";
    private ObjectMapper objectMapper = new ObjectMapper();
    //private TypeFactory typeFactory = new TypeFactory();
    private NeoServer server;
    
    private final Logger log = Logger.getLogger(LuceneSearchFunctionalTest.class.getName());

    @Before
    public void setUp() throws IOException {
        this.server = ServerBuilder.server()
                .withThirdPartyJaxRsPackage("com.okcupidlabs.neo4j.server.plugins", MOUNT_POINT)
                .build();
        this.server.start();
        LuceneSearchTestFixtures.populateDb(this.server.getDatabase().getGraph());
    }

    @Test
    public void shouldReturn400IfIndexDoesNotExist() throws IOException {
        RestRequest restRequest = new RestRequest(server.baseUri().resolve(MOUNT_POINT), CLIENT);
        JaxRsResponse response = restRequest.post("search", LuceneSearchTestFixtures.BAD_INDEX_FIXTURE);
        assertEquals(400, response.getStatus());
    }

    @Test
    public void shouldMatchNoNodes() {
        RestRequest restRequest = new RestRequest(server.baseUri().resolve(MOUNT_POINT), CLIENT);
        JaxRsResponse response = restRequest.post("search", LuceneSearchTestFixtures.NO_RESULTS_FIXTURE);
        assertEquals(200, response.getStatus());
        log.info("Got response " + response.getEntity());
    }

    @Test
    public void shouldMatchSomeNodes() {
        RestRequest restRequest = new RestRequest(server.baseUri().resolve(MOUNT_POINT), CLIENT);
        JaxRsResponse response = restRequest.post("search", LuceneSearchTestFixtures.SIM_PRESIDENT_OBAMA_FIXTURE);
        assertEquals(200, response.getStatus());
        String body = response.getEntity();
        log.info("Got response " + body);
        try {
            List responseList = objectMapper.readValue(body, List.class);
            assertEquals(6, responseList.size()); // 6 nodes about president or obama
        } catch (Exception e) {
            log.severe("Couldn't coerce response " + body + " to a list.");
            assertTrue(false); // cause a test failure
        }
    }

    @Test
    public void shouldMatchSomeNodesWithMinScore() {
        RestRequest restRequest = new RestRequest(server.baseUri().resolve(MOUNT_POINT), CLIENT);
        JaxRsResponse response = restRequest.post("search", LuceneSearchTestFixtures.PRESIDENT_OBAMA_MIN_SCORE_FIXTURE);
        assertEquals(200, response.getStatus());
        String body = response.getEntity();
        log.info("Got response " + body);
        try {
            List responseList = objectMapper.readValue(body, List.class);
            assertEquals(4, responseList.size()); // 4 high-quality nodes
        } catch (Exception e) {
            log.severe("Couldn't coerce response " + body + " to a list.");
            assertTrue(false); // cause a test failure
        }
    }

    @After
    public void tearDown() {
        this.server.stop();
    }

}
