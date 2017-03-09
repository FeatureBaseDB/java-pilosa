# Java Client for Pilosa

<img src="https://github.com/yuce/java-pilosa/blob/master/logo.png" style="float: right" align="right" height="240">

Java client for Pilosa high performance database.

## Changelog

* 2017-03-15: Initial version

## Requirements

* JDK 7 and higher
* Maven 3 and higher

## Install

### Maven

Add the following dependency in your `pom.xml`:


```xml
<dependencies>
    <dependency>
        <groupId>com.pilosa</groupId>
        <artifactId>pilosa-client</artifactId>
        <version>1.0</version>
    </dependency>
</dependencies>
```

```
mvn clean install
```

You can create an uber JAR to drop in your projects by:

```
$ git clone https://github.com/pilosa/java-pilosa.git
$ cd java-pilosa/com.pilosa.client
$ mvn clean package
```

The package is located at `target/pilosa-client-X.X.jar`

## Usage

### Quick overview

Assuming [Pilosa](https://github.com/pilosa/pilosa) server is running at `localhost:15000` (the default):

```java
// Create a client
Client client = new PilosaClient("localhost:15000");

// Send a query. PilosaException is thrown if execution of the query fails.
PilosaResponse response = client.query("example_db", "SetBit(id=5, frame='sample', profileID=42)");
// Get the result
Object result = response.getResult();
// Deai with the result

// You can send more than one query with a single query call
response = client.query("example_db",
                        "Bitmap(id=5, frame='sample')",
                        "TopN(frame='sample', n=5)");
for (Object result : response.getResults()) {
    // ...
}
```

There is a simple ORM which can be used as:
```java
response = client.query("example_db",
                        Pql.bitmap(5, "sample"),
                        Pql.topN("sample", 5);

```

## Contribution

### Running tests

Make sure you are in the `com.pilosa.client` directory.

You can run unit tests with:
```
$ mvn test
```

And integration tests with:
```
$ mvn failsafe:integration-test
```

### Generating protobuf classes

Protobuf compiler is required to generate protobuf classes:

```
$ cd com.pilosa.client/src/internal
$ protoc --java_out=../main/java/ internal.proto

```

## License
