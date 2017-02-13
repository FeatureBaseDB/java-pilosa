package com.pilosa.client;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.client.HttpClients;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;

/**
 * Pilosa HTTP client.
 *
 * <p>
 * Usage:
 *
 * <pre>
 * // Create a client
 * Client client = new Client("localhost:15000");
 * // Send a query
 * PilosaResponse response = client.query("exampleDB", "SetBit(id=5, frame=\"sample\", profileID=42)");
 * // Check whether it succeeded
 * if (response.isSuccess()) {
 *     // Get the result
 *     Object result = response.getResult();
 *     // Deai with the result
 * }
 * else {
 *     // Do something with the error message
 *     System.out.println("ERROR: " + response.getErrorMessage());
 * }
 *
 * // You can send more than one query with a single query call
 * response = client.query("exampleDB", "Bitmap(id=5, frame=\"sample\") TopN(frame=\"sample\", n=5)");
 * // Check whether it succeeded
 * if (response.isSuccess()) {
 *     // Deai with results
 *     for (Object result : response.getResults()) {
 *         System.out.println(result);
 *     }
 * }
 * </pre>
 */
public class Client {
    private static final Logger logger = LogManager.getLogger();
    private ICluster cluster;
    private boolean isConnected = false;
    private URI currentAddress;
    private HttpClient client = HttpClients.createDefault();

    /**
     * Creates a client with the given server address.
     * @param address address of the server
     * @throws URISyntaxException if the given address is malformed
     */
    public Client(String address) throws URISyntaxException {
        this(new URI(address));
    }

    /**
     * Creates a client with the given server address.
     * @param address address of the server
     * @throws URISyntaxException if the given address is malformed
     */
    public Client(URI address) throws URISyntaxException {
        this(new Cluster(address));
    }

    /**
     * Creates a client with the given cluster.
     * @param cluster contains the addreses of the servers in the cluster
     */
    public Client(ICluster cluster) {
        this.cluster = cluster;
    }

    /**
     * Queries the server with the given database name and query string.
     * @param databaseName the database to use
     * @param queryString a single query or multiple queres separated by spaces
     * @return Pilosa response
     */
    public PilosaResponse query(String databaseName, String queryString) {
        if (!this.isConnected) {
            connect();
        }
        logger.info("({}) Querying: {}", databaseName, queryString);
        String uri = this.currentAddress.toString() + "/query?db=" + databaseName;
        logger.debug("Posting to {}", uri);

        HttpPost httpPost = new HttpPost(uri);
        httpPost.setEntity(new ByteArrayEntity(queryString.getBytes(StandardCharsets.UTF_8)));
        try {
            HttpResponse response = this.client.execute(httpPost);
            HttpEntity entity = response.getEntity();
            return new PilosaResponse(entity.getContent());
        }
        catch (IOException ex) {
            logger.error(ex);
            this.cluster.removeAddress(this.currentAddress);
            this.isConnected = false;
            throw new PilosaException("Error while posting query", ex);
        }
    }

    private void connect() {
        this.currentAddress = this.cluster.getAddress();
        logger.info("Connected to {}", this.currentAddress);
        this.isConnected = true;
    }
}
