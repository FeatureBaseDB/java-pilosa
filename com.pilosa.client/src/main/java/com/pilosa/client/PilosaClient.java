package com.pilosa.client;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.pilosa.client.exceptions.*;
import com.pilosa.client.orm.Database;
import com.pilosa.client.orm.Frame;
import com.pilosa.client.orm.PqlQuery;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.client.HttpClients;
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
 * PilosaClient client = new PilosaClient("localhost:15000");
 * // Send a query. PilosaException is thrown if execution of the query fails.
 * QueryResponse response = client.query("example_db", "SetBit(id=5, frame=\"sample\", profileID=42)");
 * // Get the result
 * QueryResult result = response.getResult();
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
    private static final Logger logger = LogManager.getLogger();
    private ICluster cluster;
    private boolean isConnected = false;
    private URI currentAddress;
    private HttpClient client = null;
    private Comparator<Bit> bitComparator = new BitComparator();

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
    public QueryResponse query(String databaseName, String query) {
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
    public QueryResponse query(String databaseName, PqlQuery... queries) {
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
    public QueryResponse query(String databaseName, List<PqlQuery> queries) {
        return queryPath(QueryRequest.withDatabase(databaseName), queries);
    }

    /**
     * Runs the given query against the server.
     *
     * @param query a PqlQuery with its database is not null
     * @return Pilosa response
     * @throws ValidationException if the given query's database is null
     */
    public QueryResponse query(PqlQuery query) {
        return queryPath(QueryRequest.withQuery(query));
    }

    /**
     * Queries the server with the given database name and enables profiles in the response.
     *
     * @param databaseName the database to use
     * @param query a Pql query
     * @return Pilosa response with profiles
     * @throws ValidationException if an invalid database name is passed
     */
    public QueryResponse queryWithProfiles(String databaseName, String query) {
        QueryRequest request = QueryRequest.withDatabase(databaseName);
        request.setRetrieveProfiles(true);
        request.setQuery(query);
        return queryPath(request);
    }

    /**
     * Queries the server with the given database name and enables profiles in the response.
     * @param databaseName the database to use
     * @param queries a single or multiple PqlQuery queries
     * @return Pilosa response with profiles
     * @throws ValidationException if an invalid database name is passed
     */
    public QueryResponse queryWithProfiles(String databaseName, PqlQuery... queries) {
        QueryRequest request = QueryRequest.withDatabase(databaseName);
        request.setRetrieveProfiles(true);
        return queryPath(request, queries);
    }

    /**
     * Queries the server with the given database name and enables profiles in the response.
     *
     * @param databaseName the database to use
     * @param queries      a single or multiple PqlQuery queries
     * @return Pilosa response with profiles
     * @throws ValidationException if an invalid database name is passed
     */
    public QueryResponse queryWithProfiles(String databaseName, List<PqlQuery> queries) {
        QueryRequest request = QueryRequest.withDatabase(databaseName);
        request.setRetrieveProfiles(true);
        return queryPath(request, queries);
    }

    /**
     * Runs the given query against the server and enables profiles in the response.
     *
     * @param query a PqlQuery with its database is not null
     * @return Pilosa response
     * @throws ValidationException if the given query's database is null
     */
    public QueryResponse queryWithProfiles(PqlQuery query) {
        QueryRequest request = QueryRequest.withQuery(query);
        request.setRetrieveProfiles(true);
        return queryPath(request, query);
    }

    /**
     * Creates a database.
     *
     * @param name database name
     * @throws ValidationException     if the passed database name is not valid
     * @throws DatabaseExistsException if there already is a database with the given name
     */
    public void createDatabase(String name) {
        createDatabase(name, DatabaseOptions.withDefaults());
    }

    /**
     * Creates a database with options.
     * @param name database name
     * @param options database options
     * @throws ValidationException if the passed database name is not valid
     * @throws DatabaseExistsException if there already is a database with the given name
     */
    public void createDatabase(String name, DatabaseOptions options) {
        Validator.ensureValidDatabaseName(name);
        String uri = this.getAddress() + "/db";
        HttpPost httpPost = new HttpPost(uri);
        String body = String.format("{\"db\":\"%s\", \"options\":{\"columnLabel\":\"%s\"}}",
                name, options.getColumnLabel());
        httpPost.setEntity(new ByteArrayEntity(body.getBytes(StandardCharsets.UTF_8)));
        clientExecute(httpPost, "Error while creating database");
    }

    /**
     * Creates a database.
     * @param database database object
     * @throws ValidationException if the passed database name is not valid
     * @throws DatabaseExistsException if there already is a database with the given name
     */
    public void createDatabase(Database database) {
        createDatabase(database.getName(), database.getOptions());
    }

    /**
     * Creates a database if it does not exist
     *
     * @param database database object
     */
    public void ensureDatabaseExists(Database database) {
        try {
            createDatabase(database);
        } catch (DatabaseExistsException ex) {
            // pass
        }
    }

    /**
     * Creates a frame with the given name for the specified database
     * @param databaseName the database this frame belongs to
     * @param name frame name
     * @throws ValidationException if the passed database name or frame name is not valid
     * @throws FrameExistsException if there already a frame with the given name
     */
    public void createFrame(String databaseName, String name) {
        createFrame(databaseName, name, FrameOptions.withDefaults());
    }

    /**
     * Creates a frame with the given name and options for the specified database
     * @param databaseName the database this frame belongs to
     * @param name frame name
     * @param options frame options
     * @throws ValidationException if the passed database name or frame name is not valid
     * @throws FrameExistsException if there already a frame with the given name
     */
    public void createFrame(String databaseName, String name, FrameOptions options) {
        Validator.ensureValidDatabaseName(databaseName);
        Validator.ensureValidFrameName(name);
        String uri = this.getAddress() + "/frame";
        HttpPost httpPost = new HttpPost(uri);
        String body = String.format("{\"db\":\"%s\", \"frame\":\"%s\", \"options\":{\"rowLabel\":\"%s\"}}",
                databaseName, name, options.getRowLabel());
        httpPost.setEntity(new ByteArrayEntity(body.getBytes(StandardCharsets.UTF_8)));
        clientExecute(httpPost, "Error while creating frame");
    }

    /**
     * Creates a frame.
     * @param frame frame object
     * @throws ValidationException if the passed database name or frame name is not valid
     * @throws FrameExistsException if there already a frame with the given name
     */
    public void createFrame(Frame frame) {
        createFrame(frame.getDatabase().getName(), frame.getName(), frame.getOptions());
    }

    /**
     * Creates a frame if it does not exist
     *
     * @param frame frame object
     */
    public void ensureFrameExists(Frame frame) {
        try {
            createFrame(frame);
        } catch (FrameExistsException ex) {
            // pass
        }
    }

    /**
     * Deletes a database.
     * @param name the database to delete
     * @throws ValidationException if an invalid database name is passed
     */
    public void deleteDatabase(String name) {
        Validator.ensureValidDatabaseName(name);
        String uri = this.getAddress() + "/db";
        HttpDeleteWithBody httpDelete = new HttpDeleteWithBody(uri);
        String body = String.format("{\"db\":\"%s\"}", name);
        httpDelete.setEntity(new ByteArrayEntity(body.getBytes(StandardCharsets.UTF_8)));
        clientExecute(httpDelete, "Error while deleting database");
    }

    /**
     * Deletes a database.
     * @param database database object
     * @throws ValidationException if an invalid database name is passed
     */
    public void deleteDatabase(Database database) {
        deleteDatabase(database.getName());
    }

    /**
     * Imports bits to the given database and frame, using 1000000 as the batch size.
     *
     * @param databaseName specify the database name
     * @param frameName    specify the frame name
     * @param iterator     specify the bit iterator
     */
    public void importFrame(String databaseName, String frameName, IBitIterator iterator) {
        importFrame(databaseName, frameName, iterator, 100000);
    }


    /**
     * Imports bits to the given database and frame.
     *
     * @param databaseName specify the database name
     * @param frameName    specify the frame name
     * @param iterator     specify the bit iterator
     * @param batchSize    specify the number of bits to send in each import query
     */
    public void importFrame(String databaseName, String frameName, IBitIterator iterator, int batchSize) {
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
                importBits(databaseName, frameName, entry.getKey(), entry.getValue());
            }
        }
    }

    private String getAddress() {
        this.currentAddress = this.cluster.getAddress();
        String scheme = this.currentAddress.getScheme();
        if (!scheme.equals(HTTP)) {
            throw new PilosaException("Unknown scheme: " + scheme);
        }
        return this.currentAddress.getNormalizedAddress();
    }

    private void connect() {
        this.client = HttpClients.createDefault();
        logger.info("Connected to {}", this.currentAddress);
        this.isConnected = true;
    }

    private HttpResponse clientExecute(HttpRequestBase request, String errorMessage) {
        return clientExecute(request, errorMessage, ReturnClientResponse.NO_RESPONSE);
    }

    private HttpResponse clientExecute(HttpRequestBase request, String errorMessage,
                                       ReturnClientResponse returnResponse) {
        if (!this.isConnected) {
            connect();
        }
        try {
            HttpResponse response = client.execute(request);
            if (returnResponse != ReturnClientResponse.RAW_RESPONSE) {
                int statusCode = response.getStatusLine().getStatusCode();
                if (statusCode < 200 || statusCode >= 300) {
                    String responseError = readStream(response.getEntity().getContent());
                    // try to throw the appropriate exception
                    switch (responseError) {
                        case "database already exists\n":
                            throw new DatabaseExistsException();
                        case "frame already exists\n":
                            throw new FrameExistsException();
                    }
                    // couldn't find the exact exception, just throw a generic one
                    throw new PilosaException(String.format("Server error (%d): %s", statusCode, responseError));
                } else {
                    // the entity should be consumed, if not returned
                    if (returnResponse == ReturnClientResponse.NO_RESPONSE) {
                        EntityUtils.consume(response.getEntity());
                    }
                }
            }
            return response;
        } catch (IOException ex) {
            logger.error(ex);
            this.cluster.removeAddress(this.currentAddress);
            this.isConnected = false;
            throw new PilosaException(errorMessage, ex);
        }

    }

    private QueryResponse queryPath(QueryRequest request) {
        String uri = String.format("%s/query", this.getAddress());
        logger.debug("Posting to {}", uri);

        HttpPost httpPost;
        ByteArrayEntity body;
        httpPost = new HttpPost(uri);
        httpPost.setHeader("Content-Type", "application/x-protobuf");
        httpPost.setHeader("Accept", "application/x-protobuf");
        Internal.QueryRequest qr = request.toProtobuf();
        body = new ByteArrayEntity(qr.toByteArray());
        httpPost.setEntity(body);
        HttpResponse response = clientExecute(httpPost, "Error while posting query",
                ReturnClientResponse.RAW_RESPONSE);
        try {
            HttpEntity entity = response.getEntity();
            QueryResponse queryResponse = QueryResponse.fromProtobuf(entity.getContent());
            if (!queryResponse.isSuccess()) {
                throw new PilosaException(queryResponse.getErrorMessage());
            }
            return queryResponse;
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

    private QueryResponse queryPath(QueryRequest request, List<PqlQuery> queries) {
        StringBuilder builder = new StringBuilder(queries.size());
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
            PilosaClient client = new PilosaClient(node.toURI());
            Internal.ImportRequest importRequest = bitsToImportRequest(databaseName, frameName, slice, bits);
            client.importNode(importRequest);
        }
    }

    List<FragmentNode> fetchFrameNodes(String databaseName, long slice) {
        String addr = this.getAddress();
        String uri = String.format("%s/fragment/nodes?db=%s&slice=%d", addr, databaseName, slice);
        HttpGet httpGet = new HttpGet(uri);
        HttpResponse response = clientExecute(httpGet, "Error while fetching fragment nodes",
                ReturnClientResponse.ERROR_CHECKED_RESPONSE);
        HttpEntity entity = response.getEntity();
        ObjectMapper mapper = new ObjectMapper();
        try {
            return mapper.readValue(entity.getContent(), new TypeReference<List<FragmentNode>>() {});
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

    private String readStream(InputStream stream) throws IOException {
        ByteArrayOutputStream result = new ByteArrayOutputStream();
        byte[] buffer = new byte[1024];
        int length;
        while ((length = stream.read(buffer)) != -1) {
            result.write(buffer, 0, length);
        }
        return result.toString();
    }

    private enum ReturnClientResponse {
        RAW_RESPONSE,
        ERROR_CHECKED_RESPONSE,
        NO_RESPONSE,
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
    private String timeQuantum = "";
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
        request.setQuery(query.toString());
        return request;
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

    Internal.QueryRequest toProtobuf() {
        Internal.QueryRequest.Builder builder = Internal.QueryRequest.newBuilder()
                .setDB(this.databaseName)
                .setQuery(this.query)
                .setProfiles(this.retrieveProfiles)
                .setQuantum(this.timeQuantum);
        return builder.build();
    }
}

class FragmentNode {
    public void setHost(String host) {
        this.host = host;
    }

    URI toURI() {
        return new URI(this.host);
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
