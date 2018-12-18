# Java Client for Pilosa

<a href="https://github.com/pilosa"><img src="https://img.shields.io/badge/pilosa-1.1-blue.svg"></a>
<a href="http://search.maven.org/#search%7Cga%7C1%7Ca%3A%22pilosa-client%22"><img src="https://img.shields.io/maven-central/v/com.pilosa/pilosa-client.svg?maxAge=2592"></a>
<a href="https://travis-ci.org/pilosa/java-pilosa"><img src="https://api.travis-ci.org/pilosa/java-pilosa.svg?branch=master"></a>
<a href="https://coveralls.io/github/pilosa/java-pilosa?branch=master"><img src="https://coveralls.io/repos/github/pilosa/java-pilosa/badge.svg?branch=master" /></a>
<a href="http://javadoc.io/doc/com.pilosa/pilosa-client"><img src="http://javadoc.io/badge/com.pilosa/pilosa-client.svg" alt="Javadocs"></a>

<img src="https://www.pilosa.com/img/ee.svg" style="float: right" align="right" height="301">

Java client for Pilosa high performance distributed index.

## What's New?

See: [CHANGELOG](CHANGELOG.md)

## Requirements

* JDK 7 and higher
* Maven 3 and higher

## Install

Add the following dependency in your `pom.xml`:

```xml
<dependencies>
    <dependency>
        <groupId>com.pilosa</groupId>
        <artifactId>pilosa-client</artifactId>
        <version>1.1.0</version>
    </dependency>
</dependencies>
```

## Usage

### Quick overview

Assuming [Pilosa](https://github.com/pilosa/pilosa) server is running at `localhost:10101` (the default):

```java
// Create the default client
PilosaClient client = PilosaClient.defaultClient();

// Retrieve the schema
Schema schema = client.readSchema();

// Create an Index object
Index myindex = schema.index("myindex");

// Create a Field object
Field myfield = myindex.field("myfield");

// make sure the index and field exists on the server
client.syncSchema(schema);

// Send a Set query. PilosaException is thrown if execution of the query fails.
client.query(myfield.set(5, 42));

// Send a Row query. PilosaException is thrown if execution of the query fails.
QueryResponse response = client.query(myfield.row(5));

// Get the result
QueryResult result = response.getResult();

// Act on the result
if (result != null) {
    List<Long> columns = result.getRow().getColumns();
    System.out.println("Got columns: " + columns);
}

// You can batch queries to improve throughput
response = client.query(
    myindex.batchQuery(
        myfield.row(5),
        myfield.row(10)
    )    
);
for (Object r : response.getResults()) {
    // Act on the result
}
```

## Documentation

### Data Model and Queries

See: [Data Model and Queries](docs/data-model-queries.md)

### Executing Queries

See: [Server Interaction](docs/server-interaction.md)

### Importing and Exporting Data

See: [Importing Data](docs/imports.md)

## Contributing

See: [CONTRIBUTING](CONTRIBUTING.md)

## License

See: [LICENSE](LICENSE)
