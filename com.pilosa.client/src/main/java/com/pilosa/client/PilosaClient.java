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
import com.pilosa.client.exceptions.HttpConflict;
import com.pilosa.client.exceptions.PilosaException;
import com.pilosa.client.exceptions.PilosaURIException;
import com.pilosa.client.exceptions.ValidationException;
import com.pilosa.client.orm.*;
import com.pilosa.client.status.FieldInfo;
import com.pilosa.client.status.IFieldInfo;
import com.pilosa.client.status.IndexInfo;
import com.pilosa.client.status.SchemaInfo;
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
import java.util.concurrent.*;

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
 *     Index index = Index.create("repository");
 *     Field stargazer = index.field("stargazer");
 *     QueryResponse response = client.query(stargazer.row(5));
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
        request.setExcludeRowAttributes(options.isExcludeAttributes());
        request.setExcludeColumns(options.isExcludeColumns());
        request.setShards(options.getShards());
        return queryPath(request);
    }

    /**
     * Creates an index on the server using the given Index object.
     *
     * @param index index object
     * @throws HttpConflict if there already is a index with the given name
     * @see <a href="https://www.pilosa.com/docs/api-reference/#index-index-name-query">Pilosa API Reference: Query</a>
     * @see <a href="https://www.pilosa.com/docs/api-reference/#index-index-name">Pilosa API Reference: Index</a>
     */
    public void createIndex(Index index) {
        String path = String.format("/index/%s", index.getName());
        String body = index.getOptions().toString();
        ByteArrayEntity data = new ByteArrayEntity(body.getBytes(StandardCharsets.UTF_8));
        clientExecute("POST", path, data, null, "Error while creating index");
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
        } catch (HttpConflict ex) {
            // pass
        }
    }

    /**
     * Creates a field on the server using the given Field object.
     * @param field field object
     * @throws HttpConflict if there already is a field with the given name
     * @see <a href="https://www.pilosa.com/docs/api-reference/#index-index-name-frame-frame-name">Pilosa API Reference: Field</a>
     */
    public void createField(Field field) {
        String path = String.format("/index/%s/field/%s", field.getIndex().getName(), field.getName());
        String body = field.getOptions().toString();
        ByteArrayEntity data = new ByteArrayEntity(body.getBytes(StandardCharsets.UTF_8));
        clientExecute("POST", path, data, null, "Error while creating field");
    }

    /**
     * Creates a field on the server if it does not exist.
     *
     * @param field field object
     * @see <a href="https://www.pilosa.com/docs/api-reference/#index-index-name-frame-frame-name">Pilosa API Reference: Field</a>
     */
    public void ensureField(Field field) {
        try {
            createField(field);
        } catch (HttpConflict ex) {
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
     * Deletes the given field on the server.
     *
     * @param field the field to delete
     * @throws PilosaException if the field does not exist
     * @see <a href="https://www.pilosa.com/docs/api-reference/#index-index-name-frame-frame-name">Pilosa API Reference: Field</a>
     */
    public void deleteField(Field field) {
        String path = String.format("/index/%s/field/%s", field.getIndex().getName(), field.getName());
        clientExecute("DELETE", path, null, null, "Error while deleting field");
    }

    /**
     * Imports bits to the given index and field with the default batch size.
     *
     * @param field    specify the field
     * @param iterator     specify the bit iterator
     * @throws PilosaException if the import cannot be completed
     */
    public void importField(Field field, ColumnIterator iterator) {
        importField(field, iterator, ImportOptions.builder().build(), null);
    }

    /**
     * Imports bits to the given index and field.
     *<p>
     *     This method sorts and sends the bits in batches.
     *     Pilosa queries may return inconsistent results while importing data.
     *
     * @param field    specify the field
     * @param iterator     specify the bit iterator
     * @param options specify the import options
     * @throws PilosaException if the import cannot be completed
     * @see <a href="https://www.pilosa.com/docs/administration/#importing-and-exporting-data/">Importing and Exporting Data</a>
     */
    @SuppressWarnings("WeakerAccess")
    public void importField(Field field, ColumnIterator iterator, ImportOptions options) {
        BitImportManager manager = new BitImportManager(options);
        manager.run(this, field, iterator, null);
    }

    /**
     * Imports bits to the given index and field.
     * <p>
     * This method sorts and sends the bits in batches.
     * Pilosa queries may return inconsistent results while importing data.
     *
     * @param field       specify the field
     * @param iterator    specify the bit iterator
     * @param options     specify the import options
     * @param statusQueue specify the status queue for tracking import process
     * @throws PilosaException if the import cannot be completed
     * @see <a href="https://www.pilosa.com/docs/administration/#importing-and-exporting-data/">Importing and Exporting Data</a>
     */
    @SuppressWarnings("WeakerAccess")
    public void importField(Field field, ColumnIterator iterator, ImportOptions options, final BlockingQueue<ImportStatusUpdate> statusQueue) {
        BitImportManager manager = new BitImportManager(options);
        manager.run(this, field, iterator, statusQueue);
    }

    /**
     * Returns the schema info.
     *
     * @return SchemaInfo object.
     * @throws PilosaException if the schema cannot be read
     */
    public SchemaInfo readServerSchema() {
        String path = "/schema";
        try {
            try (CloseableHttpResponse response = clientExecute("GET", path, null, null, "Error while reading schema",
                    ReturnClientResponse.ERROR_CHECKED_RESPONSE)) {
                HttpEntity entity = response.getEntity();
                if (entity != null) {
                    try (InputStream src = entity.getContent()) {
                        return SchemaInfo.fromInputStream(src);
                    }
                }
                throw new PilosaException("Server returned empty response");
            }
        } catch (IOException ex) {
            throw new PilosaException("Error while reading response", ex);
        }
    }

    /**
     * Returns the indexes and fields on the server.
     *
     * @return server-side schema
     */
    public Schema readSchema() {
        Schema result = Schema.defaultSchema();
        SchemaInfo schema = readServerSchema();
        for (IndexInfo indexInfo : schema.getIndexes()) {
            Index index = result.index(indexInfo.getName());
            List<FieldInfo> fields = indexInfo.getFields();
            if (fields != null) {
                for (IFieldInfo fieldInfo : indexInfo.getFields()) {
                    index.field(fieldInfo.getName(), fieldInfo.getOptions());
                }
            }
        }
        return result;
    }

    /**
     * Updates a schema with the indexes and fields on the server and
     * creates the indexes and fields in the schema on the server side.
     * <p>
     * This function does not delete indexes and the fields on the server side nor in the schema.
     * </p>
     *
     * @param schema local schema to be synced
     */
    public void syncSchema(Schema schema) {
        Schema serverSchema = readSchema();

        // find out local - remote schema
        Schema diffSchema = schema.diff(serverSchema);
        // create the indexes and fields which doesn't exist on the server side
        for (Map.Entry<String, Index> indexEntry : diffSchema.getIndexes().entrySet()) {
            Index index = indexEntry.getValue();
            if (!serverSchema.getIndexes().containsKey(indexEntry.getKey())) {
                ensureIndex(index);
            }
            for (Map.Entry<String, Field> fieldEntry : index.getFields().entrySet()) {
                this.ensureField(fieldEntry.getValue());
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
                for (Map.Entry<String, Field> fieldEntry : index.getFields().entrySet()) {
                    localIndex.field(fieldEntry.getValue());
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
                            // try to throw the appropriate exception
                            if (statusCode == 409) {
                                throw new HttpConflict();
                            }
                            String responseError = readStream(src);
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
            case "POST":
                request = new HttpPost(uri);
                ((HttpPost) request).setEntity(data);
                break;
            default:
                throw new IllegalArgumentException(String.format("%s is not a valid HTTP method", method));
        }
        if (headers != null) {
            request.setHeaders(headers);
        }
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

    void importColumns(ShardColumns shardColumns) {
        String indexName = shardColumns.getIndex().getName();
        List<IFragmentNode> nodes = fetchFieldNodes(indexName, shardColumns.getShard());
        for (IFragmentNode node : nodes) {
            Cluster cluster = Cluster.withHost(node.toURI());
            PilosaClient client = this.newClientInstance(cluster, this.options);
            Internal.ImportRequest importRequest = shardColumns.convertToImportRequest();
            client.importNode(importRequest);
        }
    }

    List<IFragmentNode> fetchFieldNodes(String indexName, long shard) {
        String key = String.format("%s%d", indexName, shard);
        List<IFragmentNode> nodes;

        if (this.fragmentNodeCache == null) {
            this.fragmentNodeCache = new HashMap<>();
        } else {
            // Try to load from the cache first
            nodes = this.fragmentNodeCache.get(key);
            if (nodes != null) {
                return nodes;
            }
        }

        String path = String.format("/internal/fragment/nodes?index=%s&shard=%d", indexName, shard);
        try {
            CloseableHttpResponse response = clientExecute("GET", path, null, null, "Error while fetching fragment nodes",
                    ReturnClientResponse.ERROR_CHECKED_RESPONSE);
            HttpEntity entity = response.getEntity();
            if (entity != null) {
                try (InputStream src = response.getEntity().getContent()) {
                    nodes = mapper.readValue(src, new TypeReference<List<FragmentNode>>() {
                    });
                }
                // Cache the nodes
                this.fragmentNodeCache.put(key, nodes);
                return nodes;
            }
            throw new PilosaException("Server returned empty response");
        } catch (IOException ex) {
            throw new PilosaException("Error while reading response", ex);
        }
    }

    void importNode(Internal.ImportRequest importRequest) {
        ByteArrayEntity body = new ByteArrayEntity(importRequest.toByteArray());
        String path = String.format("/index/%s/field/%s/import", importRequest.getIndex(), importRequest.getField());
        clientExecute("POST", path, body, protobufHeaders, "Error while importing");
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
        ObjectMapper m = new ObjectMapper();
        m.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        mapper = m;

        protobufHeaders = new Header[]{
                new BasicHeader("Content-Type", "application/x-protobuf"),
                new BasicHeader("Accept", "application/x-protobuf")
        };
    }

    private static final ObjectMapper mapper;
    private static final String HTTP = "http";
    private static final String HTTPS = "https";
    private static final int MAX_HOSTS = 10;
    private static final Header[] protobufHeaders;
    private static final Logger logger = LoggerFactory.getLogger("pilosa");
    private Cluster cluster;
    private URI currentAddress;
    private CloseableHttpClient client = null;
    private ClientOptions options;
    private Map<String, List<IFragmentNode>> fragmentNodeCache = null;
}

class QueryRequest {
    private Index index;
    private String query = "";
    private boolean retrieveColumnAttributes = false;
    private boolean excludeColumns = false;
    private boolean excludeRowAttributes = false;
    private Long[] shards = {};
    private boolean useCoordinator;

    private QueryRequest(Index index) {
        this.index = index;
    }

    static QueryRequest withIndex(Index index) {
        return new QueryRequest(index);
    }

    static QueryRequest withQuery(PqlQuery query) {
        QueryRequest request = QueryRequest.withIndex(query.getIndex());
        SerializedQuery q = query.serialize();
        request.setQuery(q.getQuery());
        request.useCoordinator = q.isWriteKeys();
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

    public void setExcludeColumns(boolean excludeColumns) {
        this.excludeColumns = excludeColumns;
    }

    public void setExcludeRowAttributes(boolean excludeRowAttributes) {
        this.excludeRowAttributes = excludeRowAttributes;
    }

    public void setShards(Long... shards) {
        this.shards = shards;
    }

    Internal.QueryRequest toProtobuf() {
        return Internal.QueryRequest.newBuilder()
                .setQuery(this.query)
                .setColumnAttrs(this.retrieveColumnAttributes)
                .setExcludeColumns(this.excludeColumns)
                .setExcludeRowAttrs(this.excludeRowAttributes)
                .addAllShards(Arrays.asList(this.shards))
                .build();
    }
}

interface IFragmentNode {
    URI toURI();
}

class FragmentNode implements IFragmentNode {
    public void setURI(FragmentNodeURI uri) {
        this.uri = uri;
    }

    public URI toURI() {
        return this.uri.toURI();
    }

    private FragmentNodeURI uri = new FragmentNodeURI();
}

class FragmentNodeURI {
    @SuppressWarnings("unused")
    public void setScheme(String scheme) {
        this.scheme = scheme;
    }

    @SuppressWarnings("unused")
    public void setHost(String host) {
        this.host = host;
    }

    @SuppressWarnings("unused")
    public void setPort(int port) {
        this.port = port;
    }

    URI toURI() {
        return URI.address(String.format("%s://%s:%d", this.scheme, this.host, this.port));
    }

    private String scheme;
    private String host;
    private int port;
}

class BitComparator implements Comparator<Column> {
    @Override
    public int compare(Column column, Column other) {
        // The maximum ingestion speed is accomplished by sorting columns by row ID and then column ID
        int bitCmp = Long.signum(column.rowID - other.rowID);
        int prfCmp = Long.signum(column.columnID - other.columnID);
        return (bitCmp == 0) ? prfCmp : bitCmp;
    }
}

class BitImportManager {
    public void run(final PilosaClient client, final Field field, final ColumnIterator iterator, final BlockingQueue<ImportStatusUpdate> statusQueue) {
        final long shardWidth = this.options.getShardWidth();
        final int threadCount = this.options.getThreadCount();
        final int batchSize = this.options.getBatchSize();
        List<BlockingQueue<Column>> queues = new ArrayList<>(threadCount);
        List<Future> workers = new ArrayList<>(threadCount);

        ExecutorService service = Executors.newFixedThreadPool(threadCount);
        for (int i = 0; i < threadCount; i++) {
            BlockingQueue<Column> q = new LinkedBlockingDeque<>(batchSize);
            queues.add(q);
            Runnable worker = new BitImportWorker(client, field, q, statusQueue, this.options);
            workers.add(service.submit(worker));
        }

        try {
            // Push columns from the iterator
            while (iterator.hasNext()) {
                Column nextColumn = iterator.next();
                long shard = nextColumn.columnID / shardWidth;
                queues.get((int) (shard % threadCount)).put(nextColumn);
            }

            // Signal the threads to stop
            for (BlockingQueue<Column> q : queues) {
                q.put(Column.DEFAULT);
            }

            // Prepare to terminate the executor
            service.shutdown();

            for (Future worker : workers) {
                worker.get();
            }
        } catch (InterruptedException e) {
            for (Future worker : workers) {
                worker.cancel(true);
            }
        } catch (ExecutionException e) {
            throw new PilosaException("Error in import worker", e);
        }

    }

    BitImportManager(ImportOptions importOptions) {
        this.options = importOptions;
    }

    private final ImportOptions options;
}

class BitImportWorker implements Runnable {
    BitImportWorker(final PilosaClient client,
                    final Field field,
                    final BlockingQueue<Column> queue,
                    final BlockingQueue<ImportStatusUpdate> statusQueue,
                    final ImportOptions options) {
        this.client = client;
        this.field = field;
        this.queue = queue;
        this.statusQueue = statusQueue;
        this.options = options;
    }

    @Override
    public void run() {
        final long shardWidth = this.options.getShardWidth();
        final ImportOptions.Strategy strategy = this.options.getStrategy();
        final long timeout = this.options.getTimeoutMs();
        int batchCountDown = this.options.getBatchSize();
        long tic = System.currentTimeMillis();

        while (!Thread.currentThread().isInterrupted()) {
            try {
                Column column = this.queue.take();
                if (column.isDefaultColumn()) {
                    break;
                }
                long shard = column.getColumnID() / shardWidth;
                ShardColumns shardColumns = shardGroup.get(shard);
                if (shardColumns == null) {
                    shardColumns = ShardColumns.create(this.field, shard);
                    shardGroup.put(shard, shardColumns);
                }
                shardColumns.add(column);
                batchCountDown -= 1;
                if (strategy.equals(ImportOptions.Strategy.BATCH) && batchCountDown == 0) {
                    for (Map.Entry<Long, ShardColumns> entry : this.shardGroup.entrySet()) {
                        shardColumns = entry.getValue();
                        if (shardColumns.getColumns().size() > 0) {
                            importColumns(entry.getValue());
                        }
                    }
                    batchCountDown = this.options.getBatchSize();
                    tic = System.currentTimeMillis();
                } else if (strategy.equals(ImportOptions.Strategy.TIMEOUT) && (System.currentTimeMillis() - tic) > timeout) {
                    importColumns(shardGroup.get(largestShard()));
                    batchCountDown = this.options.getBatchSize();
                    tic = System.currentTimeMillis();
                }
            } catch (InterruptedException e) {
                break;
            }
        }
        // The thread is shutting down, import remaining columns in the batch
        for (Map.Entry<Long, ShardColumns> entry : this.shardGroup.entrySet()) {
            ShardColumns shardColumns = entry.getValue();
            if (shardColumns.getColumns().size() > 0) {
                try {
                    importColumns(entry.getValue());
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }
    }

    private long largestShard() {
        long largestCount = 0;
        long largestShard = -1;
        for (Map.Entry<Long, ShardColumns> entry : this.shardGroup.entrySet()) {
            ShardColumns shardColumns = entry.getValue();
            int shardBitCount = shardColumns.getColumns().size();
            if (shardBitCount > largestCount) {
                largestCount = shardBitCount;
                largestShard = entry.getKey();
            }
        }
        return largestShard;
    }

    private void importColumns(ShardColumns shardColumns) throws InterruptedException {
        long tic = System.currentTimeMillis();
        this.client.importColumns(shardColumns);
        if (this.statusQueue != null) {
            long tac = System.currentTimeMillis();
            ImportStatusUpdate statusUpdate = new ImportStatusUpdate(Thread.currentThread().getId(),
                    shardColumns.getShard(), shardColumns.getColumns().size(), tac - tic);
            this.statusQueue.offer(statusUpdate, 1, TimeUnit.SECONDS);
        }
        shardColumns.clear();
    }

    private final PilosaClient client;
    private final Field field;
    private final BlockingQueue<Column> queue;
    private final BlockingQueue<ImportStatusUpdate> statusQueue;
    private final ImportOptions options;
    private Map<Long, ShardColumns> shardGroup = new HashMap<>();
}
