# Java Client for Pilosa

<img src="https://github.com/yuce/java-pilosa/blob/readme/logo.png" style="float: right" align="right" height="240">

Java client for Pilosa high performance database.

## Changelog

* 2017-03-15: Initial version

## Requirements

* Java 1.7 and higher
* Maven 4 and higher

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
mvn clean
mvn install
```

## Usage

### Quick overview

Assuming [Pilosa](https://github.com/pilosa/pilosa) server is running at `localhost:15000` (the default):

```java
// Create a client
Client client = new Client("localhost:15000");
// Send a query
PilosaResponse response = client.query("exampleDB", "SetBit(id=5, frame=\"sample\", profileID=42)");
// Check whether it succeeded
if (response.isSuccess()) {
    // Get the result
    Object result = response.getResult();
    // Deai with the result
}
else {
    // Do something with the error message
    System.out.println("ERROR: " + response.getErrorMessage());
}

// You can send more than one query with a single query call
response = client.query("exampleDB", "Bitmap(id=5, frame=\"sample\") TopN(frame=\"sample\", n=5)");
// Check whether it succeeded
if (response.isSuccess()) {
    // Deai with results
    for (Object result : response.getResults()) {
        System.out.println(result);
    }
}
```

## Contribution

## License
