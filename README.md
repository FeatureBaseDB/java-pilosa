# Java Client for Pilosa

<a href="https://github.com/pilosa"><img src="https://img.shields.io/badge/pilosa-v0.4.0-blue.svg"></a>
<a href="http://search.maven.org/#search%7Cga%7C1%7Ca%3A%22pilosa-client%22"><img src="https://img.shields.io/maven-central/v/com.pilosa/pilosa-client.svg?maxAge=2592"></a>
<a href="https://travis-ci.com/pilosa/java-pilosa"><img src="https://api.travis-ci.com/pilosa/java-pilosa.svg?token=vqssvEWV3KAhu8oVFx9s&branch=master"></a>
<a href="https://coveralls.io/github/pilosa/java-pilosa?branch=master"><img src="https://coveralls.io/repos/github/pilosa/java-pilosa/badge.svg?branch=master" /></a>
<a href="http://javadoc.io/doc/com.pilosa/pilosa-client"><img src="http://javadoc.io/badge/com.pilosa/pilosa-client.svg" alt="Javadocs"></a>

<img src="https://www.pilosa.com/img/ee.svg" style="float: right" align="right" height="301">

Java client for Pilosa high performance distributed bitmap index.

## Change Log

* **Next**:
    * Failover for connection errors.    
    * More logging.
    * Uses slf4j instead of log4j for logging.
    * Introduced schemas. No need to re-define already existing indexes and frames.
    * *make* commands are supported on Windows.
    * * *Breaking Change*: Removed `timeQuantum` query option.
    * **Deprecation** `Index.withName` constructor. Use `schema.index` instead.
    * **Deprecation** `client.createIndex`, `client.createFrame`, `client.ensureIndex`, `client.ensureFrame`. Use schemas and `client.syncSchema` instead.
    
* **v0.4.0** (2017-06-09):
    * Supports Pilosa Server v0.4.0.
    * *Breaking Change*: Renamed `BatchQuery` to `PqlBatchQuery`.
    * Updated the accepted values for index, frame names and labels to match with the Pilosa server.
    * `Union` queries accept 0 or more arguments. `Intersect` and `Difference` queries accept 1 or more arguments.
    * Added `inverse TopN` and `inverse Range` calls.
    * Inverse enabled status of frames is not checked on the client side.

* **v0.3.2** (2017-05-02):
    * Available on Maven Repository.

* **v0.3.1** (2017-05-01):
    * Initial version
    * Supports Pilosa Server v0.3.1.

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
        <version>0.4.0</version>
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

// Create a Frame object
Frame myframe = myindex.frame("myframe");

// make sure the index and frame exists on the server
client.syncSchema(schema);

// Send a SetBit query. PilosaException is thrown if execution of the query fails.
client.query(myframe.setBit(5, 42));

// Send a Bitmap query. PilosaException is thrown if execution of the query fails.
QueryResponse response = client.query(myframe.bitmap(5));

// Get the result
QueryResult result = response.getResult();

// Act on the result
if (result != null) {
    List<Long> bits = result.getBitmap().getBits();
    System.out.println("Got bits: " + bits);
}

// You can batch queries to improve throughput
QueryResponse response = client.query(
    myindex.batchQuery(
        myframe.bitmap(5),
        myframe.bitmap(10)
    )    
);
for (Object result : response.getResults()) {
    // Act on the result
}
```

### Data Model and Queries

#### Indexess and Frames

*Index* and *frame*s are the main data models of Pilosa. You can check the [Pilosa documentation](https://www.pilosa.com/docs) for more detail about the data model.

`Schema.index` method is used to create an index object. Note that this does not create a index on the server; the index object simply defines the schema.

```java
Schema schema = Schema.defaultSchema();
Index repository = schema.index("repository");
```

Indexes support changing the column label and time quantum. `IndexOptions` objects store that kind of data. In order to apply these custom options, pass an `IndexOptions` object as the second argument to `Schema.index`:

```java
IndexOptions options = IndexOptions.builder()
    .setColumnLabel("repo_id")
    .setTimeQuantum(TimeQuantum.YEAR_MONTH)
    .build();

Index repository = schema.index("repository", options);
```

Frames are created with a call to `Index.frame` method:

```java
Frame stargazer = repository.frame("stargazer");
```

Similar to index objects, you can pass custom options to frames:

```java
FrameOptions stargazerOptions = FrameOptions.builder()
    .setRowLabel("stargazer_id")
    .setTimeQuantum(TimeQuantum.YEAR_MONTH_DAY)
    .build();

Frame stargazer = repository.frame("stargazer", stargazerOptions);
```

#### Queries

Once you have indexes and frame objects created, you can create queries for them. Some of the queries work on the columns; corresponding methods are attached to the index. Other queries work on rows, with related methods attached to frames.

For instance, `Bitmap` queries work on rows; use a frame object to create those queries:

```java
PqlQuery bitmapQuery = stargazer.bitmap(1, 100);  // corresponds to PQL: Bitmap(frame='stargazer', stargazer_id=1)
```

`Union` queries work on columns; use the index object to create them:

```java
PqlQuery query = repository.union(bitmapQuery1, bitmapQuery2);
```

In order to increase throughput, you may want to batch queries sent to the Pilosa server. The `index.batchQuery` method is used for that purpose:

```java
PqlQuery query = repository.batchQuery(
    stargazer.bitmap(1, 100),
    repository.union(stargazer.bitmap(100, 200), stargazer.bitmap(5, 100))
);
```

The recommended way of creating query objects is, using dedicated methods attached to index and frame objects. But sometimes it would be desirable to send raw queries to Pilosa. You can use `index.rawQuery` method for that. Note that, query string is not validated before sending to the server:

```java
PqlQuery query = repository.rawQuery("Bitmap(frame='stargazer', stargazer_id=5)");
```

Please check [Pilosa documentation](https://www.pilosa.com/docs) for PQL details. Here is a list of methods corresponding to PQL calls:

Index:

* `PqlQuery union(PqlBitmapQueries...)`
* `PqlQuery intersect(PqlBitmapQueries...)`
* `PqlQuery difference(PqlBitmapQueries...)`
* `PqlQuery count(PqlBitmapQuery bitmap)`
* `PqlQuery setColumnAttrs(long id, Map<String, Object> attributes)`

Frame:

* `PqlBitmapQuery bitmap(long rowID)`
* `PqlBitmapQuery inverseBitmap(long columnID)`
* `PqlQuery setBit(long rowID, long columnID)`
* `PqlQuery clearBit(long rowID, long columnID)`
* `PqlBitmapQuery topN(long n)`
* `PqlBitmapQuery inverseTopN(long n)`
* `PqlBitmapQuery topN(long n, PqlBitmapQuery bitmap)`
* `PqlBitmapQuery inverseTopN(long n, PqlBitmapQuery bitmap)`
* `PqlBitmapQuery topN(long n, PqlBitmapQuery bitmap, String field, Object... values)`
* `PqlBitmapQuery inverseTopN(long n, PqlBitmapQuery bitmap, String field, Object... values)`
* `PqlBitmapQuery range(long rowID, Date start, Date end)`
* `PqlBitmapQuery inverseRange(long columnID, Date start, Date end)`
* `PqlQuery setRowAttrs(long rowID, Map<String, Object> attributes)`

### Pilosa URI

A Pilosa URI has the `${SCHEME}://${HOST}:${PORT}` format:
* **Scheme**: Protocol of the URI. Default: `http`.
* **Host**: Hostname or ipv4/ipv6 IP address. Default: localhost.
* **Port**: Port number. Default: `10101`.

All parts of the URI are optional, but at least one of them must be specified. The following are equivalent:

* `http://localhost:10101`
* `http://localhost`
* `http://:10101`
* `localhost:10101`
* `localhost`
* `:10101`

A Pilosa URI is represented by the `com.pilosa.client.URI` class. Below are a few ways to create `URI` objects:

```java
import com.pilosa.client.URI;

// create the default URI: http://localhost:10101
URI uri1 = URI.defaultURI();

// create a URI from string address
URI uri2 = URI.address("db1.pilosa.com:20202");

// create a URI with the given host and port
URI uri3 = URI.fromHostPort("db1.pilosa.com", 20202);
``` 

### Pilosa Client

In order to interact with a Pilosa server, an instance of `com.pilosa.client.PilosaClient` should be created. The client is thread-safe and uses a pool of connections to the server, so we recommend creating a single instance of the client and share it with other objects when necessary.

If the Pilosa server is running at the default address (`http://localhost:10101`) you can create the client with default options using:

```java
PilosaClient client = PilosaClient.defaultClient();
```

To use a custom server address, you can use the `withAddress` class method:

```java
PilosaClient client = PilosaClient.withAddress("http://db1.pilosa.com:15000");
```

If you are running a cluster of Pilosa servers, you can create a `Cluster` object that keeps addresses of those servers:

```java
Cluster cluster = Cluster.withURI(
    URI.address(":10101"),
    URI.address(":10110"),
    URI.address(":10111"),
);

// Create a client with the cluster
PilosaClient client = PilosaClient.withCluster(cluster);
```

It is possible to customize the behaviour of the underlying HTTP client by passing a `ClientOptions` object to the `withCluster` class method:

```java
ClientOptions options = ClientOptions.builder()
    .setConnectTimeout(1000)  // if can't connect in  a second, close the connection
    .setSocketTimeout(10000)  // if no response received in 10 seconds, close the connection
    .setConnectionPoolSizePerRoute(3)  // number of connections in the pool per host
    .setConnectionPoolTotalSize(10)  // number of total connections in the pool
    .setRetryCount(5)  // number of retries before failing the request
    .build();

PilosaClient client = PilosaClient.withCluster(cluster, options);
```

Once you create a client, you can create indexes, frames and start sending queries.

Here is how you would create a index and frame:

```java
// materialize repository index and stargazer frame instances initialized before
client.syncSchema(schema);
```

You can send queries to a Pilosa server using the `query` method of client objects:

```java
QueryResponse response = client.query(frame.bitmap(5));
```

`query` method accepts an optional argument of type `QueryOptions`:

```java
QueryOptions options = QueryOptions.builder()
    .setColumns(true)  // return column data in the response
    .build();

QueryResponse response = client.query(frame.bitmap(5), options);
```

### Server Response

When a query is sent to a Pilosa server, the server either fulfills the query or sends an error message. In the case of an error, `PilosaException` is thrown, otherwise a `QueryResponse` object is returned.

A `QueryResponse` object may contain zero or more results of `QueryResult` type. You can access all results using the `getResults` method of `QueryResponse` (which returns a list of `QueryResult` objects),or you can use the `getResult` method (which returns either the first result or `null` if there are no results):

```java
QueryResponse response = client.query(frame.bitmap(5));

// check that there's a result and act on it
QueryResult result = response.getResult();
if (result != null) {
    // act on the result
}

// iterate over all results
for (QueryResult result : response.getResults()) {
    // act on the result
}
```

Similarly, a `QueryResponse` object may include a number of column objects, if `setColumns(true)` query option was used:

```java
// check that there's a column and act on it
ColumnItem column = response.getColumn();
if (column != null) {
    // act on the column
}

// iterate over all columns
for (ColumnItem column : response.getColumns()) {
    // act on the column
}
```

`QueryResult` objects contain:

* `getBitmap` method to retrieve a bitmap result,
* `getCountItems` method to retrieve column count per row ID entries returned from `topN` queries,
* `getCount` method to retrieve the number of rows per the given row ID returned from `count` queries.

```java
BitmapResult bitmap = response.getBitmap();
List<Long> bits = bitmap.getBits();
Map<String, Object> attributes = bitmap.getAttributes();

List<CountResultItem> countItems = response.getCountItems();

long count = response.getCount();
```

## Importing Data

If you have large amounts of data, it is more efficient to import it to Pilosa instead of several `SetBit` queries.
 
This library supports importing bits in the CSV (comma separated values) format:
```
ROW_ID,COLUMN_ID
```

Optionally, a timestamp with GMT time zone can be added:
```
ROW_ID,COLUMN_ID,TIMESTAMP
```

Note that, each line corresponds to a single bit and the lines end with a new line (`\n` or `\r\n`).
The target index and frame must have been created before hand.

We only ship `CsvFileBitIterator` which reads bits from a CSV (comma separated value) stream but you can create your own bit iterator by implementing `BitIterator`.

```java
String data = "1,10,683793200\n" +
              "5,20,683793300\n" +
              "3,41,683793385\n";
InputStream stream = new ByteArrayInputStream(data.getBytes("UTF-8"));
CsvFileBitIterator iterator = CsvFileBitIterator.fromStream(stream);
```

You can use `CsvFilteBiterator.fromPath` to read the bits in a file:
```java
CsvFileBitIterator iterator = CsvFileBitIterator.fromPath("/tmp/sample.csv");
```

After creating the iterator, use `PilosaClient.importFrame` method to start importing:
```java
try {
    client.importFrame(frame, iterator);
}
catch (PilosaException ex) {
    // Handle the error.
}
```

`BitIterator` extends `Iterator` interface, so you can create new iterators by implementing it. Below is a sample iterator which returns prepopulated bits:
```java
class StaticBitIterator implements BitIterator {
    private List<Bit> bits;
    private int index = 0;

    StaticBitIterator() {
        this.bits = new ArrayList<>(3);
        this.bits.add(Bit.create(1, 10, 683793200));
        this.bits.add(Bit.create(5, 20, 683793300));
        this.bits.add(Bit.create(3, 41, 683793385));
    }

    @Override
    public boolean hasNext() {
        return this.index < this.bits.size();
    }

    @Override
    public Bit next() {
        return this.bits.get(index++);
    }

    @Override
    public void remove() {
        // this is just to avoid compilation problems on JDK 7
    }
}
```

## Contribution

Please check our [Contributor's Guidelines](https://github.com/pilosa/pilosa/CONTRIBUTING.md).

1. Sign the [Developer Agreement](https://wwww.pilosa.com/developer-agreement) so we can include your contibution in our codebase.
2. Fork this repo and add it as upstream: `git remote add upstream git@github.com:pilosa/java-pilosa.git`.
3. Make sure all tests pass (use `make test-all`) and be sure that the tests cover all statements in your code (we aim for 100% test coverage).
4. Commit your code to a feature branch and send a pull request to the `master` branch of our repo.

### Running tests

You can run unit tests with:
```
make test
```

And both unit and integration tests with:
```
make test-all
```

Check the test coverage:
```
make cover
```

### Generating protobuf classes

Protobuf classes are already checked in to source control, so this step is only needed when the upstream `public.proto` changes.

Before running the following step, make sure you have the [Protobuf compiler](https://github.com/google/protobuf) installed:
```
make generate
```

### Generating documentation

The documentation can be generated using:
```
make doc
```

Generated documentation will be saved to `com.pilosa.client/target/apidocs`.

## License

```
Copyright 2017 Pilosa Corp.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions
are met:

1. Redistributions of source code must retain the above copyright
notice, this list of conditions and the following disclaimer.

2. Redistributions in binary form must reproduce the above copyright
notice, this list of conditions and the following disclaimer in the
documentation and/or other materials provided with the distribution.

3. Neither the name of the copyright holder nor the names of its
contributors may be used to endorse or promote products derived
from this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND
CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES,
INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF
MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR
CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH
DAMAGE.
```
