package com.pilosa.client;

import com.pilosa.client.exceptions.PilosaException;
import com.pilosa.client.exceptions.PilosaURIException;
import com.pilosa.client.exceptions.ValidationException;
import com.pilosa.client.internal.ClientProtos;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URLEncodedUtils;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * Pilosa HTTP client.
 *
 * <p>
 * Usage:
 *
 * <pre>
 * // Create a client
 * PilosaClient client = new PilosaClient("localhost:15000");
 * // Send a query. PilosaException is thrown if execution of the query fails.
 * PilosaResponse response = client.query("example_db", "SetBit(id=5, frame=\"sample\", profileID=42)");
 * // Get the result
 * Object result = response.getResult();
 * // Deai with the result
 *
 * // You can send more than one query with a single query call
 * response = client.query("example_db",
 *                         "Bitmap(id=5, frame=\"sample\")",
 *                         "TopN(frame=\"sample\", n=5)");
 * // Deal with results
 * for (Object result : response.getResults()) {
 *     // ...
 * }
 * </pre>
 */
public class PilosaClient {
    private static final String HTTP = "http";
    private static final String HTTP_PROTOBUF = "http+pb";
    private static final Logger logger = LogManager.getLogger();
    private ICluster cluster;
    private boolean isConnected = false;
    private URI currentAddress;
    private HttpClient client = HttpClients.createDefault();

    /**
     * Creates a client with the given server address.
     * @param address address of the server
     * @throws PilosaURIException if the given address is malformed
     */
    public PilosaClient(String address) {
        this(new URI(address));
    }

    /**
     * Creates a client with the given server address.
     * @param address address of the server
     * @throws PilosaURIException if the given address is malformed
     */
    public PilosaClient(URI address) {
        this(new Cluster(address));
    }

    /**
     * Creates a client with the given cluster.
     * @param cluster contains the addreses of the servers in the cluster
     */
    public PilosaClient(ICluster cluster) {
        this.cluster = cluster;
    }

    /**
     * Queries the server with the given database name and a query.
     * @param databaseName the database to use
     * @param query a Pql query
     * @return Pilosa response
     * @throws ValidationException if an invalid database name is passed
     */
    public PilosaResponse query(String databaseName, String query) {
        QueryRequest request = QueryRequest.withDatabase(databaseName);
        request.setQuery(query);
        return queryPath(request);
    }

    /**
     * Queries the server with the given database name and queries.
     *
     * @param databaseName the database to use
     * @param queries      a single or multiple PqlQuery queries
     * @return Pilosa response
     * @throws ValidationException if an invalid database name is passed
     */
    public PilosaResponse query(String databaseName, PqlQuery... queries) {
        return queryPath(QueryRequest.withDatabase(databaseName), queries);
    }

    /**
     * Queries the server with the given database name and queries.
     *
     * @param databaseName the database to use
     * @param queries      a single or multiple PqlQuery queries
     * @return Pilosa response
     * @throws ValidationException if an invalid database name is passed
     */
    public PilosaResponse query(String databaseName, List<PqlQuery> queries) {
        return queryPath(QueryRequest.withDatabase(databaseName), queries);
    }

    /**
     * Queries the server with the given database name and query strings.
     *
     * @param databaseName the database to use
     * @param query a Pql query
     * @return Pilosa response with profiles
     * @throws ValidationException if an invalid database name is passed
     */
    public PilosaResponse queryWithProfiles(String databaseName, String query) {
        QueryRequest request = QueryRequest.withDatabase(databaseName);
        request.setRetrieveProfiles(true);
        request.setQuery(query);
        return queryPath(request);
    }

    /**
     * Queries the server with the given database name and queries.
     * @param databaseName the database to use
     * @param queries a single or multiple PqlQuery queries
     * @return Pilosa response with profiles
     * @throws ValidationException if an invalid database name is passed
     */
    public PilosaResponse queryWithProfiles(String databaseName, PqlQuery... queries) {
        QueryRequest request = QueryRequest.withDatabase(databaseName);
        request.setRetrieveProfiles(true);
        return queryPath(request, queries);
    }

    /**
     * Queries the server with the given database name and queries.
     *
     * @param databaseName the database to use
     * @param queries      a single or multiple PqlQuery queries
     * @return Pilosa response with profiles
     * @throws ValidationException if an invalid database name is passed
     */
    public PilosaResponse queryWithProfiles(String databaseName, List<PqlQuery> queries) {
        QueryRequest request = QueryRequest.withDatabase(databaseName);
        request.setRetrieveProfiles(true);
        return queryPath(request, queries);
    }

    /**
     * Deletes a databse
     * @param name the database to delete
     * @throws ValidationException if an invalid database name is passed
     */
    public void deleteDatabase(String name) {
        Validator.ensureValidDatabaseName(name);
        if (!this.isConnected) {
            connect();
        }
        String uri = this.currentAddress.toString() + "/db";
        HttpDeleteWithBody httpDelete = new HttpDeleteWithBody(uri);
        String body = String.format("{\"db\":\"%s\"}", name);
        httpDelete.setEntity(new ByteArrayEntity(body.getBytes(StandardCharsets.UTF_8)));
        try {
            this.client.execute(httpDelete);
        } catch (IOException ex) {
            logger.error(ex);
            this.cluster.removeAddress(this.currentAddress);
            this.isConnected = false;
            throw new PilosaException("Error while deleting database", ex);
        }
    }

    private void connect() {
        this.currentAddress = this.cluster.getAddress();
        String scheme = this.currentAddress.getScheme();
        if (!scheme.equals(HTTP) && !scheme.equals(HTTP_PROTOBUF)) {
            throw new PilosaException("Unknown scheme: " + scheme);
        }
        logger.info("Connected to {}", this.currentAddress);
        this.isConnected = true;
    }

    private PilosaResponse queryPath(QueryRequest request) {
        if (!this.isConnected) {
            connect();
        }
        boolean isProtobuf = this.currentAddress.getScheme().equals(HTTP_PROTOBUF);
        String uri = String.format("%s/query", this.currentAddress.getNormalizedAddress());
        logger.debug("Posting to {}", uri);

        HttpPost httpPost;
        ByteArrayEntity body;
        if (isProtobuf) {
            httpPost = new HttpPost(uri);
            httpPost.setHeader("Content-Type", "application/x-protobuf");
            ClientProtos.QueryRequest qr = request.toProtobuf();
            body = new ByteArrayEntity(qr.toByteArray());
        } else {
            uri = String.format("%s?%s", uri, request.toURLQueryString());
            httpPost = new HttpPost(uri);
            body = new ByteArrayEntity(request.getQuery().getBytes(StandardCharsets.UTF_8));
        }
        httpPost.setEntity(body);
        try {
            HttpResponse response = this.client.execute(httpPost);
            HttpEntity entity = response.getEntity();
            PilosaResponse pilosaResponse = new PilosaResponse(entity.getContent());
            if (!pilosaResponse.isSuccess()) {
                throw new PilosaException(pilosaResponse.getErrorMessage());
            }
            return pilosaResponse;
        } catch (IOException ex) {
            logger.error(ex);
            this.cluster.removeAddress(this.currentAddress);
            this.isConnected = false;
            throw new PilosaException("Error while posting query", ex);
        }
    }

    private PilosaResponse queryPath(QueryRequest request, PqlQuery... queries) {
        StringBuilder builder = new StringBuilder(queries.length);
        for (PqlQuery query : queries) {
            builder.append(query);
        }
        request.setQuery(builder.toString());
        return queryPath(request);
    }

    private PilosaResponse queryPath(QueryRequest request, List<PqlQuery> queries) {
        StringBuilder builder = new StringBuilder(queries.size());
        for (PqlQuery query : queries) {
            builder.append(query);
        }
        request.setQuery(builder.toString());
        return queryPath(request);
    }
}

class HttpDeleteWithBody extends HttpPost {
    HttpDeleteWithBody(String url) {
        super(url);
    }

    @Override
    public String getMethod() {
        return "DELETE";
    }
}

class QueryRequest {
    private String databaseName = "";
    private String query = "";
    private String timeQuantum = null;
    private boolean retrieveProfiles = false;

    private QueryRequest(String databaseName) {
        this.databaseName = databaseName;
    }

    static QueryRequest withDatabase(String databaseName) {
        Validator.ensureValidDatabaseName(databaseName);
        return new QueryRequest(databaseName);
    }

    String getQuery() {
        return this.query;
    }

    void setQuery(String query) {
        this.query = query;
    }

    void setTimeQuantum(String quantum) {
        switch (quantum) {
            case "YMDH":
            case "YMD":
            case "YM":
            case "Y":
                this.timeQuantum = quantum;
                break;
            default:
                throw new PilosaException("Invalid time quantum: " + quantum);
        }
    }

    void setRetrieveProfiles(boolean ok) {
        this.retrieveProfiles = ok;
    }

    ClientProtos.QueryRequest toProtobuf() {
        ClientProtos.QueryRequest.Builder builder = ClientProtos.QueryRequest.newBuilder();
        builder.setDB(this.databaseName);
        builder.setQuery(this.query);
        if (this.timeQuantum != null) {
            builder.setQuantum(this.timeQuantum);
        }
        if (this.retrieveProfiles) {
            builder.setProfiles(true);
        }
        return builder.build();
    }

    String toURLQueryString() {
        List<NameValuePair> args = new ArrayList<>(3);
        args.add(new BasicNameValuePair("db", this.databaseName));
        if (this.timeQuantum != null) {
            args.add(new BasicNameValuePair("time_granularity", this.timeQuantum));
        }
        if (this.retrieveProfiles) {
            args.add(new BasicNameValuePair("profiles", "true"));
        }
        return URLEncodedUtils.format(args, '&', StandardCharsets.UTF_8);
    }


}