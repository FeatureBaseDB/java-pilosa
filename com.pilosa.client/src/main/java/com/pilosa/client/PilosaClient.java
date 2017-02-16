package com.pilosa.client;

import org.apache.commons.lang3.StringUtils;
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
 * PilosaResponse response = client.query("exampleDB", "SetBit(id=5, frame=\"sample\", profileID=42)");
 * // Get the result
 * Object result = response.getResult();
 * // Deai with the result
 *
 * // You can send more than one query with a single query call
 * response = client.query("exampleDB",
 *                         "Bitmap(id=5, frame=\"sample\")",
 *                         "TopN(frame=\"sample\", n=5)");
 * // Deal with results
 * for (Object result : response.getResults()) {
 *     // ...
 * }
 * </pre>
 */
public class PilosaClient {
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
     * Queries the server with the given database name and query string.
     * @param databaseName the database to use
     * @param queries a single query or multiple queries
     * @return Pilosa response
     */
    public PilosaResponse query(String databaseName, String... queries) {
        if (!this.isConnected) {
            connect();
        }
        String queryString = StringUtils.join(queries, " ");
        logger.debug("({}) Querying: {}", databaseName, queryString);
        String uri = this.currentAddress.toString() + "/query?db=" + databaseName;
        logger.debug("Posting to {}", uri);

        HttpPost httpPost = new HttpPost(uri);
        httpPost.setEntity(new ByteArrayEntity(queryString.getBytes(StandardCharsets.UTF_8)));
        try {
            HttpResponse response = this.client.execute(httpPost);
            HttpEntity entity = response.getEntity();
            PilosaResponse pilosaResponse = new PilosaResponse(entity.getContent());
            if (!pilosaResponse.isSuccess()) {
                throw new PilosaException(pilosaResponse.getErrorMessage());
            }
            return pilosaResponse;
        }
        catch (IOException ex) {
            logger.error(ex);
            this.cluster.removeAddress(this.currentAddress);
            this.isConnected = false;
            throw new PilosaException("Error while posting query", ex);
        }
    }

    public PilosaResponse query(String databaseName, PqlQuery... queries) {
        String[] stringQueries = new String[queries.length];
        for (int i = 0; i < queries.length; i++) {
            stringQueries[i] = queries[i].toString();
        }
        return query(databaseName, stringQueries);
    }

    public void deleteDatabase(String name) {
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
        logger.info("Connected to {}", this.currentAddress);
        this.isConnected = true;
    }
}

class HttpDeleteWithBody extends HttpPost {
    public HttpDeleteWithBody(String url) {
        super(url);
    }

    @Override
    public String getMethod() {
        return "DELETE";
    }
}