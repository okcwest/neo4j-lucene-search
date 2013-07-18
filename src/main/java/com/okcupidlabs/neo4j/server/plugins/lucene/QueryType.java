package com.okcupidlabs.neo4j.server.plugins.lucene;

/**
* A set of legal values for a querySpec PropertyMap's "type" key.
*/
public enum QueryType {
  SIM, TERM, PHRASE, GEO, NUMRANGE, BOOL, DISMAX
}