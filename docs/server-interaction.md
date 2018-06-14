# Server Interaction

## Pilosa URI

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

## Pilosa Client

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
Cluster cluster = Cluster.withHost(
    URI.address(":10101"),
    URI.address(":10110"),
    URI.address(":10111")
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

Once you create a client, you can create indexes, fields and start sending queries.

Here is how you would create a index and field:

```java
Schema schema = client.readSchema();
Index index = schema.index("index");
Frame field = index.field("field");
client.syncSchema(schema);
```

You can send queries to a Pilosa server using the `query` method of client objects:

```java
QueryResponse response = client.query(field.bitmap(5));
```

`query` method accepts an optional argument of type `QueryOptions`:

```java
QueryOptions options = QueryOptions.builder()
    .setColumns(true)  // return column data in the response
    .build();

QueryResponse response = client.query(field.bitmap(5), options);
```

## Server Response

When a query is sent to a Pilosa server, the server either fulfills the query or sends an error message. In the case of an error, `PilosaException` is thrown, otherwise a `QueryResponse` object is returned.

A `QueryResponse` object may contain zero or more results of `QueryResult` type. You can access all results using the `getResults` method of `QueryResponse` (which returns a list of `QueryResult` objects),or you can use the `getResult` method (which returns either the first result or `null` if there are no results):

```java
QueryResponse response = client.query(field.bitmap(5));

// check that there's a result and act on it
QueryResult result = response.getResult();
if (result != null) {
    // act on the result
}

// iterate over all results
for (QueryResult r : response.getResults()) {
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
* `getCountItems` method to retrieve column count per row ID entries returned from `TopN` queries,
* `getCount` method to retrieve the number of rows per the given row ID returned from `Count` queries.
* `getValue` method to retrieve the result of `Min`, `Max` or `Sum` queries.
* `isChanged` method returns whether a `SetBit` or `ClearBit` query changed a bit.

```java
QueryResult result = response.getResult();
BitmapResult bitmap = result.getBitmap();
List<Long> bits = bitmap.getBits();
Map<String, Object> attributes = bitmap.getAttributes();

List<CountResultItem> countItems = result.getCountItems();

long count = result.getCount();

long value = result.getValue();

boolean changed = result.isChanged();
```

## SSL/TLS

Make sure the Pilosa server runs on a TLS address. [How To Set Up a Secure Cluster](https://www.pilosa.com/docs/latest/tutorials/#how-to-set-up-a-secure-cluster) tutorial explains how to do that.

In order to enable TLS support on the client side, the scheme of the address should be explicitly specified as `https`, e.g.: `https://01.pilosa.local:10501`

This client library uses the [Apache HTTP Library](https://hc.apache.org). `ClientOptions` builder accepts a `javax.net.ssl.SSLContext` object, which is set to `org.apache.http.ssl.SSLContexts.createDefault()` by default. If the Pilosa server is using a certificate from a recognized authority, you can use the defaults. 
  
If you are using a self signed certificate, you need to derive from `PilosaClient` and override the `getRegistry` method: 
```java
public class InsecurePilosaClient extends PilosaClient {

    public InsecurePilosaClient(Cluster cluster, ClientOptions options) {
        super(cluster, options);
    }

    @Override
    protected Registry<ConnectionSocketFactory> getRegistry() {
        HostnameVerifier verifier = new HostnameVerifier() {
            @Override
            public boolean verify(String hostName, SSLSession session) {
                return true;
            }
        };

        SSLContext sslContext = null;
        try {
            sslContext = new SSLContextBuilder().loadTrustMaterial(null, new TrustStrategy() {
                public boolean isTrusted(X509Certificate[] arg0, String arg1) throws CertificateException {
                    return true;
                }
            }).build();
        } catch (NoSuchAlgorithmException | KeyManagementException | KeyStoreException e) {
            e.printStackTrace();
        }
        if (sslContext == null) {
            throw new RuntimeException("SSL Context not created");
        }

        SSLConnectionSocketFactory sslConnectionSocketFactory = new SSLConnectionSocketFactory(
                sslContext,
                new String[]{"TLSv1.2"}, null, verifier);
        return RegistryBuilder.<ConnectionSocketFactory>create()
                .register("http", PlainConnectionSocketFactory.getSocketFactory())
                .register("https", sslConnectionSocketFactory)
                .build();
    }
}
```
