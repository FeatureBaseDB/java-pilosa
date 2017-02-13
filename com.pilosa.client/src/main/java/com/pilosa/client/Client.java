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
import java.io.UnsupportedEncodingException;
import java.net.URISyntaxException;

public class Client {
    private static final Logger logger = LogManager.getLogger();
    private ICluster cluster;
    private boolean isConnected = false;
    private URI currentAddress;
    private HttpClient client = HttpClients.createDefault();

    public Client(String address) throws URISyntaxException {
        this(new URI(address));
    }

    public Client(URI address) throws URISyntaxException {
        this(new Cluster(address));
    }

    public Client(ICluster cluster) {
        this.cluster = cluster;
    }

    public PilosaResponse query(String databaseName, String queryString) {
        if (!this.isConnected) {
            connect();
        }
        logger.info("({}) Querying: {}", databaseName, queryString);
        String uri = this.currentAddress.toString() + "/query?db=" + databaseName;
        logger.debug("Posting to {}", uri);

        HttpPost httpPost = new HttpPost(uri);
        try {
            httpPost.setEntity(new ByteArrayEntity(queryString.getBytes("UTF-8")));
            HttpResponse response = this.client.execute(httpPost);
            HttpEntity entity = response.getEntity();
            return new PilosaResponse(entity.getContent());
        }
        catch (UnsupportedEncodingException ex) {
            logger.error(ex);
            return PilosaResponse.error(ex.toString());
        }
        catch (IOException ex) {
            logger.error(ex);
            this.cluster.removeAddress(this.currentAddress);
            this.isConnected = false;
            return PilosaResponse.error(ex.toString());
        }
    }

    private void connect() {
        this.currentAddress = this.cluster.getAddress();
        logger.info("Connected to {}", this.currentAddress);
        this.isConnected = true;
    }
}
