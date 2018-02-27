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

import org.apache.http.ssl.SSLContexts;

import javax.net.ssl.SSLContext;

/**
 * Contains options to customize {@link PilosaClient}.
 * <p>
 * In order to set options, create a {@link ClientOptions.Builder} object using {@link ClientOptions#builder()}.
 * <p>
 * <pre>
 * <code>
 *     ClientOptions options = ClientOptions.builder()
 *          .setSocketTimeout(10000)
 *          .setConnectTimeout(100)
 *          .build();
 * </code>
 * </pre>
 */
@SuppressWarnings("WeakerAccess")
public final class ClientOptions {
    public static class Builder {
        private Builder() {
        }

        /**
         * Sets the timeout for waiting for data from the socket.
         *
         * @param millis timeout in milliseconds
         * @return ClientOptions builder object
         */
        public Builder setSocketTimeout(int millis) {
            this.socketTimeout = millis;
            return this;
        }

        /**
         * Sets the timeout for establishing a connection.
         *
         * @param millis timeout in milliseconds
         * @return ClientOptions builder object
         */
        public Builder setConnectTimeout(int millis) {
            this.connectTimeout = millis;
            return this;
        }

        /**
         * Sets the number of retries before failing a request.
         * @param count number of retries
         * @return ClientOptions builder object
         */
        public Builder setRetryCount(int count) {
            this.retryCount = count;
            return this;
        }

        /**
         * Sets the number of maximum connections per host.
         * <p>
         *     Determines the number of concurrent requests that can run against a Pilosa host.
         *
         * @param size maximum number of connections per host
         * @return ClientOptions builder object
         */
        public Builder setConnectionPoolSizePerRoute(int size) {
            this.connectionPoolSizePerRoute = size;
            return this;
        }

        /**
         * Sets the number of maximum total connections.
         * <p>
         *     Determines the number of concurrent requests that can run against a Pilosa cluster.
         *
         * @param size maximum number of connections per cluster
         * @return ClientOptions builder object
         */
        public Builder setConnectionPoolTotalSize(int size) {
            this.connectionPoolTotalSize = size;
            return this;
        }

        public Builder setSslContext(SSLContext sslContext) {
            this.sslContext = sslContext;
            return this;
        }

        public Builder setSkipVersionCheck() {
            this.skipVersionCheck = true;
            return this;
        }

        public Builder setLegacyMode(boolean enable) {
            this.legacyMode = enable;
            this.skipVersionCheck = true;
            return this;
        }

        /**
         * Creates the ClientOptions object.
         * @return ClientOptions object
         */
        public ClientOptions build() {
            return new ClientOptions(this.socketTimeout, this.connectTimeout,
                    this.retryCount, this.connectionPoolSizePerRoute, this.connectionPoolTotalSize,
                    this.sslContext, this.skipVersionCheck, this.legacyMode);
        }

        private int socketTimeout = 300000;
        private int connectTimeout = 30000;
        private int retryCount = 3;
        private int connectionPoolSizePerRoute = 10;
        private int connectionPoolTotalSize = 100;
        private SSLContext sslContext = SSLContexts.createDefault();
        private boolean skipVersionCheck = false;
        private boolean legacyMode = false;
    }

    /**
     * Creates a ClientOptions.Builder object.
     * @return a Builder object
     */
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

    public SSLContext getSslContext() {
        return this.sslContext;
    }

    public boolean isSkipVersionCheck() {
        return this.skipVersionCheck;
    }

    public boolean isLegacyMode() {
        return this.legacyMode;
    }

    private ClientOptions(final int socketTimeout, final int connectTimeout, final int retryCount,
                          final int connectionPoolSizePerRoute, final int connectionPoolTotalSize,
                          final SSLContext sslContext, final boolean skipVersionCheck, final boolean legacyMode) {
        this.socketTimeout = socketTimeout;
        this.connectTimeout = connectTimeout;
        this.retryCount = retryCount;
        this.connectionPoolSizePerRoute = connectionPoolSizePerRoute;
        this.connectionPoolTotalSize = connectionPoolTotalSize;
        this.sslContext = sslContext;
        this.skipVersionCheck = skipVersionCheck;
        this.legacyMode = legacyMode;
    }

    private final int socketTimeout; // milliseconds
    private final int connectTimeout; // milliseconds
    private final int retryCount;
    private final int connectionPoolSizePerRoute;
    private final int connectionPoolTotalSize;
    private final SSLContext sslContext;
    private final boolean legacyMode;
    private final boolean skipVersionCheck;
}
