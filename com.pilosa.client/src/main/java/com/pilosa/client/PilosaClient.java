/*
 * Copyright 2017 Pilosa Corp.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 *
 * 1. Redistributions of source code must retain the above copyright
 * notice, this list of conditions and the following disclaimer.
 *
 * 2. Redistributions in binary form must reproduce the above copyright
 * notice, this list of conditions and the following disclaimer in the
 * documentation and/or other materials provided with the distribution.
 *
 * 3. Neither the name of the copyright holder nor the names of its
 * contributors may be used to endorse or promote products derived
 * from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND
 * CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES,
 * INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR
 * CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
 * BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
 * SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
 * WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
 * NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH
 * DAMAGE.
 */

package com.pilosa.client;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.pilosa.client.exceptions.*;
import com.pilosa.client.orm.Frame;
import com.pilosa.client.orm.Index;
import com.pilosa.client.orm.PqlQuery;
import com.pilosa.client.orm.Schema;
import com.pilosa.client.status.FrameInfo;
import com.pilosa.client.status.IndexInfo;
import com.pilosa.client.status.NodeInfo;
import com.pilosa.client.status.StatusInfo;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.*;
import org.apache.http.config.Registry;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.conn.socket.ConnectionSocketFactory;
import org.apache.http.conn.socket.PlainConnectionSocketFactory;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.message.BasicHeader;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.HostnameVerifier;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Constructor;
import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * Pilosa HTTP client.
 *<p>
 *     This client uses Pilosa's http+protobuf API.
 * <p>
 * Usage:
 * <pre>
 * <code>
 *     // Create a PilosaClient instance
 *     PilosaClient client = PilosaClient.defaultClient();
 *     // Create an Index instance
 *     Index index = Index.withName("repository");
 *     Frame stargazer = index.frame("stargazer");
 *     QueryResponse response = client.query(stargazer.bitmap(5));
 *     // Act on the result
 *     System.out.println(response.getResult());
 * </code>
 * </pre>
 * @see <a href="https://www.pilosa.com/docs/api-reference/">Pilosa API Reference</a>
 * @see <a href="https://www.pilosa.com/docs/query-language/">Query Language</a>
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
     * Creates a client with the given address and options.
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
     * @param query a PqlBaseQuery with its index is not null
     * @return Pilosa response
     * @throws ValidationException if the given query's index is null
     * @see <a href="https://www.pilosa.com/docs/api-reference/#index-index-name-query">Pilosa API Reference: Query</a>
     */
    public QueryResponse query(PqlQuery query) {
        return query(query, QueryOptions.defaultOptions());
    }

    /**
     * Runs the given query against the server with the given options.
     *
     * @param query a PqlQuery object with a non-null index
     * @return Pilosa response
     * @throws ValidationException if the given query's index is null
     * @see <a href="https://www.pilosa.com/docs/api-reference/#index-index-name-query">Pilosa API Reference: Query</a>
     */
    public QueryResponse query(PqlQuery query, QueryOptions options) {
        QueryRequest request = QueryRequest.withQuery(query);
        request.setRetrieveColumnAttributes(options.isColumns());
        request.setExcludeAttributes(options.isExcludeAttributes());
        request.setExcludeBits(options.isExcludeBits());
        request.setSlices(options.getSlices());
        request.setQuery(query.serialize());
        return queryPath(request);
    }

    /**
     * Creates an index on the server using the given Index object.
     *
     * @param index index object
     * @throws IndexExistsException if there already is a index with the given name
     * @see <a href="https://www.pilosa.com/docs/api-reference/#index-index-name-query">Pilosa API Reference: Query</a>
     * @see <a href="https://www.pilosa.com/docs/api-reference/#index-index-name">Pilosa API Reference: Index</a>
     */
    public void createIndex(Index index) {
        String path = String.format("/index/%s", index.getName());
        String body = index.getOptions().toString();
        ByteArrayEntity data = new ByteArrayEntity(body.getBytes(StandardCharsets.UTF_8));
        clientExecute("POST", path, data, protobufHeaders, "Error while creating index");
        // set time quantum for the index if one was assigned to it
        if (index.getOptions().getTimeQuantum() != TimeQuantum.NONE) {
            patchTimeQuantum(index);
        }
    }

    /**
     * Creates an index on the server if it does not exist.
     *
     * @param index index object
     * @see <a href="https://www.pilosa.com/docs/api-reference/#index-index-name">Pilosa API Reference: Index</a>
     */
    public void ensureIndex(Index index) {
        try {
            createIndex(index);
        } catch (IndexExistsException ex) {
            // pass
        }
    }

    /**
     * Creates a frame on the server using the given Frame object.
     * @param frame frame object
     * @throws FrameExistsException if there already is a frame with the given name
     * @see <a href="https://www.pilosa.com/docs/api-reference/#index-index-name-frame-frame-name">Pilosa API Reference: Frame</a>
     */
    public void createFrame(Frame frame) {
        String path = String.format("/index/%s/frame/%s", frame.getIndex().getName(), frame.getName());
        String body = frame.getOptions().toString();
        ByteArrayEntity data = new ByteArrayEntity(body.getBytes(StandardCharsets.UTF_8));
        clientExecute("POST", path, data, protobufHeaders, "Error while creating frame");
    }

    /**
     * Creates a frame on the server if it does not exist.
     *
     * @param frame frame object
     * @see <a href="https://www.pilosa.com/docs/api-reference/#index-index-name-frame-frame-name">Pilosa API Reference: Frame</a>
     */
    public void ensureFrame(Frame frame) {
        try {
            createFrame(frame);
        } catch (FrameExistsException ex) {
            // pass
        }
    }

    /**
     * Deletes the given index on the server.
     * @param index the index to delete
     * @throws PilosaException if the index does not exist
     * @see <a href="https://www.pilosa.com/docs/api-reference/#index-index-name">Pilosa API Reference: Index</a>
     */
    public void deleteIndex(Index index) {
        String path = String.format("/index/%s", index.getName());
        clientExecute("DELETE", path, null, null, "Error while deleting index");
    }

    /**
     * Deletes the given frame on the server.
     *
     * @param frame the frame to delete
     * @throws PilosaException if the frame does not exist
     * @see <a href="https://www.pilosa.com/docs/api-reference/#index-index-name-frame-frame-name">Pilosa API Reference: Frame</a>
     */
    public void deleteFrame(Frame frame) {
        String path = String.format("/index/%s/frame/%s", frame.getIndex().getName(), frame.getName());
        clientExecute("DELETE", path, null, null, "Error while deleting frame");
    }

    /**
     * Imports bits to the given index and frame with the default batch size.
     *
     * @param frame    specify the frame
     * @param iterator     specify the bit iterator
     * @throws PilosaException if the import cannot be completed
     */
    public void importFrame(Frame frame, BitIterator iterator) {
        importFrame(frame, iterator, 100000);
    }

    /**
     * Imports bits to the given index and frame.
     *<p>
     *     This method sorts and sends the bits in batches.
     *     Pilosa queries may return inconsistent results while importing data.
     *
     * @param frame    specify the frame
     * @param iterator     specify the bit iterator
     * @param batchSize    specify the number of bits to send in each import request
     * @throws PilosaException if the import cannot be completed
     * @see <a href="https://www.pilosa.com/docs/administration/#importing-and-exporting-data/">Importing and Exporting Data</a>
     */
    @SuppressWarnings("WeakerAccess")
    public void importFrame(Frame frame, BitIterator iterator, int batchSize) {
        final long sliceWidth = 1048576L;
        boolean canContinue = true;
        while (canContinue) {
            // The maximum ingestion speed is accomplished by sorting bits by row ID and then column ID
            Map<Long, List<Bit>> bitGroup = new HashMap<>();
            for (int i = 0; i < batchSize; i++) {
                if (iterator.hasNext()) {
                    Bit bit = iterator.next();
                    long slice = bit.getColumnID() / sliceWidth;
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
                importBits(frame.getIndex().getName(), frame.getName(), entry.getKey(), entry.getValue());
            }
        }
    }

    /**
     * Returns the status of the cluster and schema info.
     *
     * @return StatusInfo object.
     * @throws PilosaException if the status cannot be read
     */
    public StatusInfo readStatus() {
        String path = "/status";
        CloseableHttpResponse response = null;
        try {
            try {
                response = clientExecute("GET", path, null, null, "Error while reading status",
                        ReturnClientResponse.ERROR_CHECKED_RESPONSE);
                HttpEntity entity = response.getEntity();
                if (entity != null) {
                    try (InputStream src = entity.getContent()) {
                        StatusMessage msg = StatusMessage.fromInputStream(src);
                        return msg.getStatus();
                    }
                }
                throw new PilosaException("Server returned empty response");
            } finally {
                if (response != null) {
                    response.close();
                }
            }
        } catch (IOException ex) {
            throw new PilosaException("Error while reading response", ex);
        }
    }

    /**
     * Returns the indexes and frames on the server.
     *
     * @return server-side schema
     */
    public Schema readSchema() {
        Schema result = Schema.defaultSchema();
        StatusInfo status = readStatus();
        for (NodeInfo nodeInfo : status.getNodes()) {
            for (IndexInfo indexInfo : nodeInfo.getIndexes()) {
                Index index = result.index(indexInfo.getName(), indexInfo.getOptions());
                for (FrameInfo frameInfo : indexInfo.getFrames()) {
                    index.frame(frameInfo.getName(), frameInfo.getOptions());
                }
            }
        }
        return result;
    }

    /**
     * Updates a schema with the indexes and frames on the server and
     * creates the indexes and frames in the schema on the server side.
     * <p>
     * This function does not delete indexes and the frames on the server side nor in the schema.
     * </p>
     *
     * @param schema local schema to be synced
     */
    public void syncSchema(Schema schema) {
        Schema serverSchema = readSchema();

        // find out local - remote schema
        Schema diffSchema = schema.diff(serverSchema);
        // create the indexes and frames which doesn't exist on the server side
        for (Map.Entry<String, Index> indexEntry : diffSchema.getIndexes().entrySet()) {
            Index index = indexEntry.getValue();
            if (!serverSchema.getIndexes().containsKey(indexEntry.getKey())) {
                ensureIndex(index);
            }
            for (Map.Entry<String, Frame> frameEntry : index.getFrames().entrySet()) {
                this.ensureFrame(frameEntry.getValue());
            }
        }

        // find out remote - local schema
        diffSchema = serverSchema.diff(schema);
        for (Map.Entry<String, Index> indexEntry : diffSchema.getIndexes().entrySet()) {
            String indexName = indexEntry.getKey();
            Index index = indexEntry.getValue();
            if (!schema.getIndexes().containsKey(indexName)) {
                schema.index(index);
            } else {
                Index localIndex = schema.getIndexes().get(indexName);
                for (Map.Entry<String, Frame> frameEntry : index.getFrames().entrySet()) {
                    localIndex.frame(frameEntry.getValue());
                }
            }
        }
    }

    /**
     * Sends an HTTP request to the Pilosa server.
     *
     * @param method HTTP request method (GET, POST, PATCH, DELETE, ...)
     * @param path   HTTP request path.
     * @return the response to this request.
     */
    public CloseableHttpResponse httpRequest(final String method, final String path) {
        return httpRequest(method, path, null, null);
    }

    /**
     * Sends an HTTP request to the Pilosa server.
     * <p>
     * <b>NOTE</b>: This function is experimental and may be removed in later revisions.
     * </p>
     *
     * @param method  HTTP request method (GET, POST, PATCH, DELETE, ...)
     * @param path    HTTP request path.
     * @param data    HTTP request body.
     * @param headers HTTP request headers.
     * @return the response to this request.
     */
    public CloseableHttpResponse httpRequest(final String method, final String path, final ByteArrayEntity data,
                                             Header[] headers) {
        return clientExecute(method, path, data, headers, "HTTP request error", ReturnClientResponse.RAW_RESPONSE);
    }

    protected PilosaClient(Cluster cluster, ClientOptions options) {
        this.cluster = cluster;
        this.options = options;
    }

    protected PilosaClient newClientInstance(Cluster cluster, ClientOptions options) {
        // find the constructor with the correct arguments
        try {
            Constructor constructor = this.getClass().getDeclaredConstructor(Cluster.class, ClientOptions.class);
            return (PilosaClient) constructor.newInstance(cluster, options);
        } catch (Exception e) {
            throw new RuntimeException("This PilosaClient descendant does not have the correct constructor");
        }
    }

    protected Registry<ConnectionSocketFactory> getRegistry() {
        HostnameVerifier verifier = SSLConnectionSocketFactory.getDefaultHostnameVerifier();
        SSLConnectionSocketFactory sslConnectionSocketFactory = new SSLConnectionSocketFactory(
                this.options.getSslContext(),
                new String[]{"TLSv1.2"}, null, verifier);
        return RegistryBuilder.<ConnectionSocketFactory>create()
                .register("http", PlainConnectionSocketFactory.getSocketFactory())
                .register("https", sslConnectionSocketFactory)
                .build();
    }

    private String getAddress() {
        this.currentAddress = this.cluster.getHost();
        String scheme = this.currentAddress.getScheme();
        if (!scheme.equals(HTTP) && !scheme.equals(HTTPS)) {
            throw new PilosaException("Unknown scheme: " + scheme);
        }
        logger.debug("Current host set: {}", this.currentAddress);
        return this.currentAddress.getNormalized();
    }

    private void connect() {
        PoolingHttpClientConnectionManager cm = new PoolingHttpClientConnectionManager(getRegistry());
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
    }

    private void clientExecute(final String method, final String path, final ByteArrayEntity data,
                               Header[] headers, String errorMessage) {
        clientExecute(method, path, data, headers, errorMessage, ReturnClientResponse.NO_RESPONSE);
    }

    private CloseableHttpResponse clientExecute(final String method, final String path, final ByteArrayEntity data,
                                                Header[] headers, String errorMessage, ReturnClientResponse returnResponse) {
        if (this.client == null) {
            connect();
        }
        CloseableHttpResponse response = null;
        // try at most MAX_HOSTS non-failed hosts; protect against broken cluster.removeHost
        for (int i = 0; i < MAX_HOSTS; i++) {
            HttpRequestBase request = makeRequest(method, path, data, headers);
            logger.debug("Request: {} {}", request.getMethod(), request.getURI());
            try {
                response = client.execute(request);
                break;
            } catch (IOException ex) {
                this.cluster.removeHost(this.currentAddress);
                logger.warn("Removed {} from the cluster due to {}", this.currentAddress, ex);
                this.currentAddress = null;
            }
        }
        if (response == null) {
            throw new PilosaException(String.format("Tried %s hosts, still failing", MAX_HOSTS));
        }
        try {
            if (returnResponse != ReturnClientResponse.RAW_RESPONSE) {
                int statusCode = response.getStatusLine().getStatusCode();
                if (statusCode < 200 || statusCode >= 300) {
                    HttpEntity entity = response.getEntity();
                    if (entity != null) {
                        try (InputStream src = entity.getContent()) {
                            String responseError = readStream(src);
                            // try to throw the appropriate exception
                            switch (responseError) {
                                case "index already exists\n":
                                    throw new IndexExistsException();
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
                    } finally {
                        response.close();
                    }
                }
            }
            return response;
        } catch (IOException ex) {
            throw new PilosaException(errorMessage, ex);
        }
    }

    HttpRequestBase makeRequest(final String method, final String path, final ByteArrayEntity data, final Header[] headers) {
        HttpRequestBase request;
        String uri = this.getAddress() + path;
        switch (method) {
            case "GET":
                request = new HttpGet(uri);
                break;
            case "DELETE":
                request = new HttpDelete(uri);
                break;
            case "PATCH":
                request = new HttpPatch(uri);
                ((HttpPatch) request).setEntity(data);
                break;
            case "POST":
                request = new HttpPost(uri);
                ((HttpPost) request).setEntity(data);
                break;
            default:
                throw new IllegalArgumentException(String.format("%s is not a valid HTTP method", method));
        }
        request.setHeaders(headers);
        return request;
    }

    private QueryResponse queryPath(QueryRequest request) {
        String path = String.format("/index/%s/query", request.getIndex().getName());
        Internal.QueryRequest qr = request.toProtobuf();
        ByteArrayEntity body = new ByteArrayEntity(qr.toByteArray());
        try {
            CloseableHttpResponse response = clientExecute("POST", path, body, protobufHeaders, "Error while posting query",
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

    private void importBits(String indexName, String frameName, long slice, List<Bit> bits) {
        Collections.sort(bits, bitComparator);
        List<FragmentNode> nodes = fetchFrameNodes(indexName, slice);
        for (FragmentNode node : nodes) {
            Cluster cluster = Cluster.withHost(node.toURI());
            PilosaClient client = this.newClientInstance(cluster, this.options);
            Internal.ImportRequest importRequest = bitsToImportRequest(indexName, frameName, slice, bits);
            client.importNode(importRequest);
        }
    }

    List<FragmentNode> fetchFrameNodes(String indexName, long slice) {
        String path = String.format("/fragment/nodes?index=%s&slice=%d", indexName, slice);
        try {
            CloseableHttpResponse response = clientExecute("GET", path, null, null, "Error while fetching fragment nodes",
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
        ByteArrayEntity body = new ByteArrayEntity(importRequest.toByteArray());
        clientExecute("POST", "/import", body, protobufHeaders, "Error while importing");
    }

    private Internal.ImportRequest bitsToImportRequest(String indexName, String frameName, long slice,
                                                       List<Bit> bits) {
        List<Long> bitmapIDs = new ArrayList<>(bits.size());
        List<Long> columnIDs = new ArrayList<>(bits.size());
        List<Long> timestamps = new ArrayList<>(bits.size());
        for (Bit bit : bits) {
            bitmapIDs.add(bit.getRowID());
            columnIDs.add(bit.getColumnID());
            timestamps.add(bit.getTimestamp());
        }
        return Internal.ImportRequest.newBuilder()
                .setIndex(indexName)
                .setFrame(frameName)
                .setSlice(slice)
                .addAllRowIDs(bitmapIDs)
                .addAllColumnIDs(columnIDs)
                .addAllTimestamps(timestamps)
                .build();
    }

    private void patchTimeQuantum(Index index) {
        String path = String.format("/index/%s/time-quantum", index.getName());
        String body = String.format("{\"timeQuantum\":\"%s\"}",
                index.getOptions().getTimeQuantum().toString());
        ByteArrayEntity data = new ByteArrayEntity(body.getBytes(StandardCharsets.UTF_8));
        clientExecute("PATCH", path, data, protobufHeaders, "Error while setting time quantum for the index");
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
        return String.format("java-pilosa/%s", Version.getVersion());
    }

    private enum ReturnClientResponse {
        RAW_RESPONSE,
        ERROR_CHECKED_RESPONSE,
        NO_RESPONSE,
    }

    static {
        protobufHeaders = new Header[]{
                new BasicHeader("Content-Type", "application/x-protobuf"),
                new BasicHeader("Accept", "application/x-protobuf")
        };
    }

    private static final String HTTP = "http";
    private static final String HTTPS = "https";
    private static final int MAX_HOSTS = 10;
    private static final Header[] protobufHeaders;
    private static final Logger logger = LoggerFactory.getLogger("pilosa");
    private Cluster cluster;
    private URI currentAddress;
    private CloseableHttpClient client = null;
    private Comparator<Bit> bitComparator = new BitComparator();
    private ClientOptions options;
}

class QueryRequest {
    private Index index;
    private String query = "";
    private boolean retrieveColumnAttributes = false;
    private boolean excludeBits = false;
    private boolean excludeAttributes = false;
    private Long[] slices = {};

    private QueryRequest(Index index) {
        this.index = index;
    }

    static QueryRequest withIndex(Index index) {
        return new QueryRequest(index);
    }

    static QueryRequest withQuery(PqlQuery query) {
        QueryRequest request = QueryRequest.withIndex(query.getIndex());
        request.setQuery(query.serialize());
        return request;
    }

    String getQuery() {
        return this.query;
    }

    Index getIndex() {
        return this.index;
    }

    void setQuery(String query) {
        this.query = query;
    }

    void setRetrieveColumnAttributes(boolean ok) {
        this.retrieveColumnAttributes = ok;
    }

    public void setExcludeBits(boolean excludeBits) {
        this.excludeBits = excludeBits;
    }

    public void setExcludeAttributes(boolean excludeAttributes) {
        this.excludeAttributes = excludeAttributes;
    }

    public void setSlices(Long... slices) {
        this.slices = slices;
    }

    Internal.QueryRequest toProtobuf() {
        return Internal.QueryRequest.newBuilder()
                .setQuery(this.query)
                .setColumnAttrs(this.retrieveColumnAttributes)
                .setExcludeBits(this.excludeBits)
                .setExcludeAttrs(this.excludeAttributes)
                .addAllSlices(Arrays.asList(this.slices))
                .build();
    }
}

class FragmentNode {
    @SuppressWarnings("unused")
    public void setHost(String host) {
        this.host = host;
    }

    public void setScheme(String scheme) {
        this.scheme = scheme;
    }

    @SuppressWarnings("unused")
    public void setInternalHost(String host) {
        // internal host is used for internode communication
        // just adding this no op so jackson doesn't complain... --YT
    }

    URI toURI() {
        return URI.address(String.format("%s://%s", this.scheme, this.host));
    }

    private String host;
    private String scheme;
}

class BitComparator implements Comparator<Bit> {
    @Override
    public int compare(Bit bit, Bit other) {
        int bitCmp = Long.signum(bit.getRowID() - other.getRowID());
        int prfCmp = Long.signum(bit.getColumnID() - other.getColumnID());
        return (bitCmp == 0) ? prfCmp : bitCmp;
    }
}

final class StatusMessage {

    static StatusMessage fromInputStream(InputStream src) throws IOException {
        return mapper.readValue(src, StatusMessage.class);
    }

    StatusInfo getStatus() {
        return this.status;
    }

    void setStatus(StatusInfo status) {
        this.status = status;
    }

    static {
        mapper = new ObjectMapper();
        StatusMessage.mapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
    }

    private StatusInfo status;
    private final static ObjectMapper mapper;
}
