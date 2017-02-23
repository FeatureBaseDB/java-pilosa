package com.pilosa.client;

import com.pilosa.client.exceptions.PilosaException;
import com.pilosa.client.exceptions.PilosaURIException;
import com.pilosa.client.exceptions.ValidationException;
import com.pilosa.client.internal.ClientProtos;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.client.HttpClients;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
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
        String path = String.format("/query?db=%s", databaseName);
        return queryPath(path, databaseName, query);
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
        String path = String.format("/query?db=%s", databaseName);
        return queryPath(path, databaseName, queries);
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
        String path = String.format("/query?db=%s", databaseName);
        return queryPath(path, databaseName, queries);
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
        String path = String.format("/query?db=%s&profiles=true", databaseName);
        return queryPath(path, databaseName, query);
    }

    /**
     * Queries the server with the given database name and queries.
     * @param databaseName the database to use
     * @param queries a single or multiple PqlQuery queries
     * @return Pilosa response with profiles
     * @throws ValidationException if an invalid database name is passed
     */
    public PilosaResponse queryWithProfiles(String databaseName, PqlQuery... queries) {
        String path = String.format("/query?db=%s&profiles=true", databaseName);
        return queryPath(path, databaseName, queries);
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
        String path = String.format("/query?db=%s&profiles=true", databaseName);
        return queryPath(path, databaseName, queries);
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
        if (!scheme.equals(HTTP) || !scheme.equals(HTTP_PROTOBUF)) {
            throw new PilosaException("Unknown scheme: " + scheme);
        }
        logger.info("Connected to {}", this.currentAddress);
        this.isConnected = true;
    }

    private PilosaResponse queryPath(String path, String databaseName, String queryString) {
        Validator.ensureValidDatabaseName(databaseName);
        if (!this.isConnected) {
            connect();
        }
        String uri = this.currentAddress + path;
        logger.debug("Posting to {}", uri);

        HttpPost httpPost = new HttpPost(uri);
        ByteArrayEntity body;
        if (this.currentAddress.getScheme().equals(HTTP_PROTOBUF)) {
            httpPost.setHeader("Content-Type", "application/x-protobuf");
            ClientProtos.QueryRequest qr = ClientProtos.QueryRequest.newBuilder()
                    .setDB(databaseName)
                    .setQuery(queryString)
                    .build();
            body = new ByteArrayEntity(qr.toByteArray());
        } else {
            body = new ByteArrayEntity(queryString.getBytes(StandardCharsets.UTF_8));
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

    private PilosaResponse queryPath(String path, String databaseName, PqlQuery... queries) {
        StringBuilder builder = new StringBuilder(queries.length);
        for (PqlQuery query : queries) {
            builder.append(query);
        }
        return queryPath(path, databaseName, builder.toString());
    }

    private PilosaResponse queryPath(String path, String databaseName, List<PqlQuery> queries) {
        StringBuilder builder = new StringBuilder(queries.size());
        for (PqlQuery query : queries) {
            builder.append(query);
        }
        return queryPath(path, databaseName, builder.toString());
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