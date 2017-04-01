package com.pilosa.client;

public class ClientOptions {
    public ClientOptions() {
    }

    public int getSocketTimeout() {
        return socketTimeout;
    }

    public void setSocketTimeout(int socketTimeout) {
        this.socketTimeout = socketTimeout;
    }

    public int getConnectTimeout() {
        return connectTimeout;
    }

    public void setConnectTimeout(int connectTimeout) {
        this.connectTimeout = connectTimeout;
    }

    public int getRetryCount() {
        return retryCount;
    }

    public void setRetryCount(int retryCount) {
        this.retryCount = retryCount;
    }

    public int getConnectionPoolSizePerRoute() {
        return connectionPoolSizePerRoute;
    }

    public void setConnectionPoolSizePerRoute(int connectionPoolSizePerRoute) {
        this.connectionPoolSizePerRoute = connectionPoolSizePerRoute;
    }

    public int getConnectionPoolTotalSize() {
        return connectionPoolTotalSize;
    }

    public void setConnectionPoolTotalSize(int connectionPoolTotalSize) {
        this.connectionPoolTotalSize = connectionPoolTotalSize;
    }

    private int socketTimeout = 300000; // milliseconds
    private int connectTimeout = 30000; // milliseconds
    private int retryCount = 3;
    private int connectionPoolSizePerRoute = 10;
    private int connectionPoolTotalSize = 100;
}
