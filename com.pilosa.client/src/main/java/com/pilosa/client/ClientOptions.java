package com.pilosa.client;

@SuppressWarnings("WeakerAccess")
public final class ClientOptions {
    public static class Builder {
        private Builder() {
        }

        public Builder setSocketTimeout(int millis) {
            this.socketTimeout = millis;
            return this;
        }

        public Builder setConnectTimeout(int millis) {
            this.connectTimeout = millis;
            return this;
        }

        public Builder setRetryCount(int count) {
            this.retryCount = count;
            return this;
        }

        public Builder setConnectionPoolSizePerRoute(int size) {
            this.connectionPoolSizePerRoute = size;
            return this;
        }

        public Builder setConnectionPoolTotalSize(int size) {
            this.connectionPoolTotalSize = size;
            return this;
        }

        public ClientOptions build() {
            return new ClientOptions(this.socketTimeout, this.connectTimeout,
                    this.retryCount, this.connectionPoolSizePerRoute, this.connectionPoolTotalSize);
        }

        private int socketTimeout = 300000;
        private int connectTimeout = 30000;
        private int retryCount = 3;
        private int connectionPoolSizePerRoute = 10;
        private int connectionPoolTotalSize = 100;
    }

    public static Builder builder() {
        return new Builder();
    }

    public int getSocketTimeout() {
        return socketTimeout;
    }

    public int getConnectTimeout() {
        return connectTimeout;
    }

    public int getRetryCount() {
        return retryCount;
    }

    public int getConnectionPoolSizePerRoute() {
        return connectionPoolSizePerRoute;
    }

    public int getConnectionPoolTotalSize() {
        return connectionPoolTotalSize;
    }

    private ClientOptions(final int socketTimeout, final int connectTimeout, final int retryCount,
                          final int connectionPoolSizePerRoute, final int connectionPoolTotalSize) {
        this.socketTimeout = socketTimeout;
        this.connectTimeout = connectTimeout;
        this.retryCount = retryCount;
        this.connectionPoolSizePerRoute = connectionPoolSizePerRoute;
        this.connectionPoolTotalSize = connectionPoolTotalSize;
    }

    private final int socketTimeout; // milliseconds
    private final int connectTimeout; // milliseconds
    private final int retryCount;
    private final int connectionPoolSizePerRoute;
    private final int connectionPoolTotalSize;
}
