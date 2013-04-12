# Lucene Search

## Overview

Perform complex queries against a Lucene index.

## Installation

Installation is quite simple, but in order to build the extension you will need a working Java environment with Maven2
already installed.

### Building

First, clone the Git repository locally, then change to the repository directory and issue:

```
mvn package
```

Once the build/test phases complete successfully you should have output like the following:

```
Results :

Tests run: 8, Failures: 0, Errors: 0, Skipped: 0

[INFO]
[INFO] --- maven-jar-plugin:2.3.1:jar (default-jar) @ neo4j-lucene-search ---
[INFO] Building jar: /Users/barnett/workspace/neo4j-lucene-search/target/neo4j-lucene-search-0.1.jar
[INFO] ------------------------------------------------------------------------
[INFO] BUILD SUCCESS
[INFO] ------------------------------------------------------------------------
[INFO] Total time: 8.240s
[INFO] Finished at: Thu Mar 07 14:42:09 PST 2013
[INFO] Final Memory: 14M/81M
[INFO] ------------------------------------------------------------------------
```

The important line is:

```
[INFO] Building jar: /Users/barnett/workspace/neo4j-lucene-search/target/neo4j-lucene-search-0.1.jar
```

which points you to the resulting JAR.

### Install to Neo4j Plugins

Installation of the JAR to Neo4j requires copying the JAR into the Neo4j server's library path and editing a config file
to inform the Neo4j server of a new package containing API endpoints.

Copy the JAR to your server's plugins directory (replace $NEO4J_PATH with your Neo4j install directory)

```
cp neo4j-lucene-similarity-search-0.1.jar $NEO4J_PATH/plugins/
```

then edit $NEO4J_PATH/conf/neo4j-server.properties and find the line

```
#org.neo4j.server.thirdparty_jaxrs_classes=org.neo4j.examples.server.unmanaged=/examples/unmanaged
```

uncomment it edit it to look like:

```
org.neo4j.server.thirdparty_jaxrs_classes=com.okcupidlabs.neo4j.server.plugins.lucene=/lucene
```

This informs the Neo4j server to mount our extension API endpoints anchored from the server root /.  By changing "/" to
any valid URL path fragment you can mount the extension URLs anywhere you would like.

## Usage

The plugin provides a single endpoint, /search, which allows for a complex query to be specified against a given index.

#### Description

Queries the named index using the specified query, which may be complex and nested.

#### Methods

POST

#### Parameters
<dl>
  <dt>index_name
  <dd>Name of node index to query. This node index must already exist.

  <dt>query_spec
  <dd>A complex query specification, as given below.

  <dt><i>min_score (optional)</i>
  <dd>A threshold similarity score to trim off poor-quality results.
</dl>

The query_spec describes a query which may be nested indefinitely. It must contain a "type" string which is one of (DISMAX|BOOL|TERM|SIM), and may contain an optional "boost" numeric value. Additional fields are type-specific and are as follows:

{"type": "DISMAX"
 "subqueries": [$QUERY0 $QUERY1 ... $QUERYN]    // where $QUERYX is another valid query spec.
 "tiebreaker": $tiebreakingvalue                // this field is optional
 }

{"type": "BOOL"
 "clauses": [
    {"query_spec": $QUERY0 // where $QUERYX is another valid query spec
     "occurs": (MUST|SHOULD|MUST_NOT)
     }
    ...
    ]
 }

{"type": "TERM"
 "index_key": $FIELD_TO_SEARCH_IN
 "query": $TERM_TO_FIND
 }
 
{"type": "SIM"
 "index_key": $FIELD_TO_SEARCH_IN
 "query": $TERM_TO_FIND
 }


