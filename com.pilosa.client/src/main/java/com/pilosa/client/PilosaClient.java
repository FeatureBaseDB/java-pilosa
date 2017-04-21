package com.pilosa.client;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.pilosa.client.exceptions.*;
import com.pilosa.client.orm.Database;
import com.pilosa.client.orm.Frame;
import com.pilosa.client.orm.PqlQuery;
import org.apache.http.HttpEntity;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.*;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.util.EntityUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * Pilosa HTTP client.
 *
 * <p>
 * Usage:
 *
 * <pre>
 * // Create a client
 * PilosaClient client = new PilosaClient("localhost:10101");
 * // Send a query. PilosaException is thrown if execution of the query fails.
 * QueryResponse response = client.query("example_db", "SetBit(id=5, frame=\"sample\", profileID=42)");
 * // Get the result
 * QueryResult result = response.getResult();
 * // Deai with the result
 *
 * </pre>
 */
public class PilosaClient implements AutoCloseable {
    /**
     * Creates a client with the default address and options.
     *
     * @return a PilosaClient
     */
    @SuppressWarnings("WeakerAccess")
    public static PilosaClient defaultClient() {
        return PilosaClient.withURI(URI.defaultURI());
    }

    /**
     * Creates a client with the given address and options
     *
     * @param address of the Pilosa server
     * @return a PilosaClient
     */
    public static PilosaClient withAddress(String address) {
        return PilosaClient.withURI(URI.address(address));
    }

    /**
     * Creates a client with the given server address.
     * @param uri address of the server
     * @throws PilosaURIException if the given address is malformed
     * @return a PilosaClient
     */
    public static PilosaClient withURI(URI uri) {
        return PilosaClient.withCluster(Cluster.withHost(uri));
    }

    /**
     * Creates a client with the given cluster and default options.
     * @param cluster contains the addresses of the servers in the cluster
     * @return a PilosaClient
     */
    public static PilosaClient withCluster(Cluster cluster) {
        return PilosaClient.withCluster(cluster, ClientOptions.builder().build());
    }

    /**
     * Creates a client with the given cluster and options.
     * @param cluster contains the addresses of the servers in the cluster
     * @param options client options
     * @return a PilosaClient
     */
    @SuppressWarnings("WeakerAccess")
    public static PilosaClient withCluster(Cluster cluster, ClientOptions options) {
        return new PilosaClient(cluster, options);
    }

    public void close() throws IOException {
        if (this.client != null) {
            this.client.close();
            this.client = null;
        }
    }

    /**
     * Runs the given query against the server.
     *
     * @param query a PqlBaseQuery with its database is not null
     * @return Pilosa response
     * @throws ValidationException if the given query's database is null
     */
    public QueryResponse query(PqlQuery query) {
        return query(query, QueryOptions.defaultOptions());
    }

    /**
     * Runs the given query against the server and enables profiles in the response.
     *
     * @param query a PqlBaseQuery with its database is not null
     * @return Pilosa response
     * @throws ValidationException if the given query's database is null
     */
    public QueryResponse query(PqlQuery query, QueryOptions options) {
        QueryRequest request = QueryRequest.withQuery(query);
        request.setRetrieveProfiles(options.isProfiles());
        return queryPath(request, query);
    }

    /**
     * Creates a database.
     * @param database database object
     * @throws ValidationException if the passed database name is not valid
     * @throws DatabaseExistsException if there already is a database with the given name
     */
    public void createDatabase(Database database) {
        String uri = String.format("%s/db/%s", this.getAddress(), database.getName());
        HttpPost httpPost = new HttpPost(uri);
        String body = database.getOptions().toString();
        httpPost.setEntity(new ByteArrayEntity(body.getBytes(StandardCharsets.UTF_8)));
        clientExecute(httpPost, "Error while creating database");

        // set time quantum for the database if one was assigned to it
        if (database.getOptions().getTimeQuantum() != TimeQuantum.NONE) {
            patchTimeQuantum(database);
        }
    }

    /**
     * Creates a database if it does not exist
     *
     * @param database database object
     */
    public void ensureDatabase(Database database) {
        try {
            createDatabase(database);
        } catch (DatabaseExistsException ex) {
            // pass
        }
    }

    /**
     * Creates a frame.
     * @param frame frame object
     * @throws ValidationException if the passed database name or frame name is not valid
     * @throws FrameExistsException if there already a frame with the given name
     */
    public void createFrame(Frame frame) {
        String uri = String.format("%s/db/%s/frame/%s", this.getAddress(),
                frame.getDatabase().getName(), frame.getName());
        HttpPost httpPost = new HttpPost(uri);
        String body = frame.getOptions().toString();
        httpPost.setEntity(new ByteArrayEntity(body.getBytes(StandardCharsets.UTF_8)));
        clientExecute(httpPost, "Error while creating frame");

        // set time quantum for the frame if one was assigned to it
        if (frame.getOptions().getTimeQuantum() != TimeQuantum.NONE) {
            patchTimeQuantum(frame);
        }
    }

    /**
     * Creates a frame if it does not exist
     *
     * @param frame frame object
     */
    public void ensureFrame(Frame frame) {
        try {
            createFrame(frame);
        } catch (FrameExistsException ex) {
            // pass
        }
    }

    /**
     * Deletes a database.
     * @param database database object
     */
    public void deleteDatabase(Database database) {
        String uri = String.format("%s/db/%s", this.getAddress(), database.getName());
        HttpDeleteWithBody httpDelete = new HttpDeleteWithBody(uri);
        clientExecute(httpDelete, "Error while deleting database");
    }

    /**
     * Deletes a frame.
     *
     * @param frame frame object
     */
    public void deleteFrame(Frame frame) {
        String uri = String.format("%s/db/%s/frame/%s", this.getAddress(),
                frame.getDatabase().getName(), frame.getName());
        HttpDeleteWithBody httpDelete = new HttpDeleteWithBody(uri);
        clientExecute(httpDelete, "Error while deleting frame");
    }

    /**
     * Imports bits to the given database and frame.
     *
     * @param frame    specify the frame
     * @param iterator     specify the bit iterator
     */
    public void importFrame(Frame frame, IBitIterator iterator) {
        importFrame(frame, iterator, 100000);
    }

    /**
     * Imports bits to the given database and frame.
     *
     * @param frame    specify the frame
     * @param iterator     specify the bit iterator
     * @param batchSize    specify the number of bits to send in each import query
     */
    @SuppressWarnings("WeakerAccess")
    public void importFrame(Frame frame, IBitIterator iterator, int batchSize) {
        final long sliceWidth = 1048576L;
        boolean canContinue = true;
        while (canContinue) {
            // The maximum ingestion speed is accomplished by sorting bits by bitmap ID and then profile ID
            Map<Long, List<Bit>> bitGroup = new HashMap<>();
            for (int i = 0; i < batchSize; i++) {
                if (iterator.hasNext()) {
                    Bit bit = iterator.next();
                    long slice = bit.getProfileID() / sliceWidth;
                    List<Bit> sliceList = bitGroup.get(slice);
                    if (sliceList == null) {
                        sliceList = new ArrayList<>(1);
                        bitGroup.put(slice, sliceList);
                    }
                    sliceList.add(bit);

                } else {
                    canContinue = false;
                    break;
                }
            }
            for (Map.Entry<Long, List<Bit>> entry : bitGroup.entrySet()) {
                importBits(frame.getDatabase().getName(), frame.getName(), entry.getKey(), entry.getValue());
            }
        }
    }

    private String getAddress() {
        this.currentAddress = this.cluster.getHost();
        String scheme = this.currentAddress.getScheme();
        if (!scheme.equals(HTTP)) {
            throw new PilosaException("Unknown scheme: " + scheme);
        }
        return this.currentAddress.getNormalized();
    }

    private void connect() {
        PoolingHttpClientConnectionManager cm = new PoolingHttpClientConnectionManager();
        cm.setDefaultMaxPerRoute(this.options.getConnectionPoolSizePerRoute());
        cm.setMaxTotal(this.options.getConnectionPoolTotalSize());
        RequestConfig requestConfig = RequestConfig.custom()
                .setConnectTimeout(this.options.getConnectTimeout())
                .setSocketTimeout(this.options.getSocketTimeout())
                .build();
        this.client = HttpClients.custom()
                .setConnectionManager(cm)
                .setDefaultRequestConfig(requestConfig)
                .setUserAgent(makeUserAgent())
                .build();
        logger.info("Connected to {}", this.currentAddress);
    }

    private CloseableHttpResponse clientExecute(HttpRequestBase request, String errorMessage) {
        return clientExecute(request, errorMessage, ReturnClientResponse.NO_RESPONSE);
    }

    private CloseableHttpResponse clientExecute(HttpRequestBase request, String errorMessage,
                                       ReturnClientResponse returnResponse) {
        if (this.client == null) {
            connect();
        }
        try {
            CloseableHttpResponse response = client.execute(request);
            if (returnResponse != ReturnClientResponse.RAW_RESPONSE) {
                int statusCode = response.getStatusLine().getStatusCode();
                if (statusCode < 200 || statusCode >= 300) {
                    HttpEntity entity = response.getEntity();
                    if (entity != null) {
                        try (InputStream src = entity.getContent()) {
                            String responseError = readStream(src);
                            // try to throw the appropriate exception
                            switch (responseError) {
                                case "database already exists\n":
                                    throw new DatabaseExistsException();
                                case "frame already exists\n":
                                    throw new FrameExistsException();
                            }
                            // couldn't find the exact exception, just throw a generic one
                            throw new PilosaException(String.format("Server error (%d): %s", statusCode, responseError));
                        }
                    }
                    throw new PilosaException(String.format("Server error (%d): empty response", statusCode));
                }
                // the entity should be consumed, if not returned
                if (returnResponse == ReturnClientResponse.NO_RESPONSE) {
                    try {
                        EntityUtils.consume(response.getEntity());
                    }
                    finally {
                        response.close();
                    }
                }
            }
            return response;
        } catch (IOException ex) {
            logger.error(ex);
            this.cluster.removeHost(this.currentAddress);
            throw new PilosaException(errorMessage, ex);
        }
    }

    private QueryResponse queryPath(QueryRequest request) {
        String uri = String.format("%s/db/%s/query", this.getAddress(),
                request.getDatabaseName());
        logger.debug("Posting to {}", uri);

        HttpPost httpPost;
        ByteArrayEntity body;
        httpPost = new HttpPost(uri);
        httpPost.setHeader("Content-Type", "application/x-protobuf");
        httpPost.setHeader("Accept", "application/x-protobuf");
        Internal.QueryRequest qr = request.toProtobuf();
        body = new ByteArrayEntity(qr.toByteArray());
        httpPost.setEntity(body);
        try {
            CloseableHttpResponse response = clientExecute(httpPost, "Error while posting query",
                    ReturnClientResponse.RAW_RESPONSE);
            HttpEntity entity = response.getEntity();
            if (entity != null) {
                try (InputStream src = entity.getContent()) {
                    QueryResponse queryResponse = QueryResponse.fromProtobuf(src);
                    if (!queryResponse.isSuccess()) {
                        throw new PilosaException(queryResponse.getErrorMessage());
                    }
                    return queryResponse;
                }
            }
            throw new PilosaException("Server returned empty response");
        } catch (IOException ex) {
            throw new PilosaException("Error while reading response", ex);
        }
    }

    private QueryResponse queryPath(QueryRequest request, PqlQuery... queries) {
        StringBuilder builder = new StringBuilder(queries.length);
        for (PqlQuery query : queries) {
            builder.append(query);
        }
        request.setQuery(builder.toString());
        return queryPath(request);
    }

    private void importBits(String databaseName, String frameName, long slice, List<Bit> bits) {
        Collections.sort(bits, bitComparator);
        List<FragmentNode> nodes = fetchFrameNodes(databaseName, slice);
        for (FragmentNode node : nodes) {
            PilosaClient client = PilosaClient.withURI(node.toURI());
            Internal.ImportRequest importRequest = bitsToImportRequest(databaseName, frameName, slice, bits);
            client.importNode(importRequest);
        }
    }

    List<FragmentNode> fetchFrameNodes(String databaseName, long slice) {
        String addr = this.getAddress();
        String uri = String.format("%s/fragment/nodes?db=%s&slice=%d", addr, databaseName, slice);
        HttpGet httpGet = new HttpGet(uri);
        try {
            CloseableHttpResponse response = clientExecute(httpGet, "Error while fetching fragment nodes",
                    ReturnClientResponse.ERROR_CHECKED_RESPONSE);
            HttpEntity entity = response.getEntity();
            if (entity != null) {
                try (InputStream src = response.getEntity().getContent()) {
                    ObjectMapper mapper = new ObjectMapper();
                    return mapper.readValue(src, new TypeReference<List<FragmentNode>>() {
                    });
                }
            }
            throw new PilosaException("Server returned empty response");
        } catch (IOException ex) {
            throw new PilosaException("Error while reading response", ex);
        }
    }

    void importNode(Internal.ImportRequest importRequest) {
        String uri = String.format("%s/import", this.getAddress());
        HttpPost httpPost = new HttpPost(uri);
        ByteArrayEntity body = new ByteArrayEntity(importRequest.toByteArray());
        httpPost.setHeader("Content-Type", "application/x-protobuf");
        httpPost.setHeader("Accept", "application/x-protobuf");
        httpPost.setEntity(body);
        clientExecute(httpPost, "Error while importing");
    }

    private Internal.ImportRequest bitsToImportRequest(String databaseName, String frameName, long slice,
                                                       List<Bit> bits) {
        List<Long> bitmapIDs = new ArrayList<>(bits.size());
        List<Long> profileIDs = new ArrayList<>(bits.size());
        List<Long> timestamps = new ArrayList<>(bits.size());
        for (Bit bit : bits) {
            bitmapIDs.add(bit.getBitmapID());
            profileIDs.add(bit.getProfileID());
            timestamps.add(bit.getTimestamp());
        }
        return Internal.ImportRequest.newBuilder()
                .setDB(databaseName)
                .setFrame(frameName)
                .setSlice(slice)
                .addAllBitmapIDs(bitmapIDs)
                .addAllProfileIDs(profileIDs)
                .addAllTimestamps(timestamps)
                .build();
    }

    private void patchTimeQuantum(Database database) {
        String uri = String.format("%s/db/%s/time-quantum", this.getAddress(), database.getName());
        HttpPatch httpPatch = new HttpPatch(uri);
        String body = String.format("{\"time_quantum\":\"%s\"}",
                database.getOptions().getTimeQuantum().getStringValue());
        httpPatch.setEntity(new ByteArrayEntity(body.getBytes(StandardCharsets.UTF_8)));
        clientExecute(httpPatch, "Error while setting time quantum for the database");
    }

    private void patchTimeQuantum(Frame frame) {
        String uri = String.format("%s/db/%s/frame/%s/time-quantum", this.getAddress(),
                frame.getDatabase().getName(), frame.getName());
        HttpPatch httpPatch = new HttpPatch(uri);
        String body = String.format("{\"time_quantum\":\"%s\"}",
                frame.getOptions().getTimeQuantum().getStringValue());
        httpPatch.setEntity(new ByteArrayEntity(body.getBytes(StandardCharsets.UTF_8)));
        clientExecute(httpPatch, "Error while setting time quantum for the database");
    }

    private String readStream(InputStream stream) throws IOException {
        ByteArrayOutputStream result = new ByteArrayOutputStream();
        byte[] buffer = new byte[1024];
        int length;
        while ((length = stream.read(buffer)) != -1) {
            result.write(buffer, 0, length);
        }
        return result.toString();
    }

    private String makeUserAgent() {
        return String.format("java-pilosa/%s;%s", Version.getVersion(), Version.getBuildTime());
    }

    private enum ReturnClientResponse {
        RAW_RESPONSE,
        ERROR_CHECKED_RESPONSE,
        NO_RESPONSE,
    }

    private PilosaClient(Cluster cluster, ClientOptions options) {
        this.cluster = cluster;
        this.options = options;
    }

    private static final String HTTP = "http";
    private static final Logger logger = LogManager.getLogger();
    private Cluster cluster;
    private URI currentAddress;
    private CloseableHttpClient client = null;
    private Comparator<Bit> bitComparator = new BitComparator();
    private ClientOptions options;
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
    private TimeQuantum timeQuantum = TimeQuantum.NONE;
    private boolean retrieveProfiles = false;

    private QueryRequest(String databaseName) {
        this.databaseName = databaseName;
    }

    static QueryRequest withDatabase(String databaseName) {
        Validator.ensureValidDatabaseName(databaseName);
        return new QueryRequest(databaseName);
    }

    static QueryRequest withQuery(PqlQuery query) {
        // We call QueryRequest.withDatabase in order to protect against database name == null
        // TODO: check that database name is not null and create the QueryRequest object directly.
        QueryRequest request = QueryRequest.withDatabase(query.getDatabase().getName());
        request.setQuery(query.serialize());
        return request;
    }

    String getQuery() {
        return this.query;
    }

    String getDatabaseName() {
        return this.databaseName;
    }

    void setQuery(String query) {
        this.query = query;
    }

    void setTimeQuantum(TimeQuantum timeQuantum) {
        this.timeQuantum = timeQuantum;
    }

    void setRetrieveProfiles(boolean ok) {
        this.retrieveProfiles = ok;
    }

    Internal.QueryRequest toProtobuf() {
        Internal.QueryRequest.Builder builder = Internal.QueryRequest.newBuilder()
                .setQuery(this.query)
                .setProfiles(this.retrieveProfiles)
                .setQuantum(this.timeQuantum.getStringValue());
        return builder.build();
    }
}

class FragmentNode {
    public void setHost(String host) {
        this.host = host;
    }

    URI toURI() {
        return URI.address(this.host);
    }

    private String host;
}

class BitComparator implements Comparator<Bit> {
    @Override
    public int compare(Bit bit, Bit other) {
        int bitCmp = Long.signum(bit.getBitmapID() - other.getBitmapID());
        int prfCmp = Long.signum(bit.getProfileID() - other.getProfileID());
        return (bitCmp == 0) ? prfCmp : bitCmp;
    }
}
