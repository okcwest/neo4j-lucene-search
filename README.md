# Lucene Similarity Search

## Overview

Allows a Lucene similarity search to be performed against any index. The plugin will attempt to discover the analyzer type used by the index, and fail over to a WhitespaceAnalyzer if the index's analyzer cannot be instantiated.

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
[INFO] --- maven-jar-plugin:2.3.1:jar (default-jar) @ neo4j-lucene-similarity-search ---
[INFO] Building jar: /Users/barnett/workspace/neo4j-lucene-text-search/target/neo4j-lucene-similarity-search-0.1.jar
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
[INFO] Building jar: /Users/barnett/workspace/neo4j-lucene-text-search/target/neo4j-lucene-similarity-search-0.1.jar
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

This plugin provides a single endpoint for executing a similarity search:

`/lucene/simsearch`

#### Description

Queries the named index and key to find indexed nodes similar to the given text query.

#### Methods

POST

#### Parameters
<dl>
  <dt>index_name
  <dd>Name of node index to query. This node index must already exist.

  <dt>index_key
  <dd>Index key to inspect.

  <dt>query
  <dd>The text to match.

  <dt><i>min_score (optional)</i>
  <dd>A threshold similarity score to trim off poor-quality results.
</dl>

