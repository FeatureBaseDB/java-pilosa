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

public class ImportOptions {

    public enum Strategy {
        DEFAULT,
        TIMEOUT,
        BATCH
    }

    public static class Builder {
        private Builder() {
        }

        public ImportOptions build() {
            return new ImportOptions(this.threadCount,
                    this.timeoutMs, this.batchSize, this.strategy);
        }

        public Builder setThreadCount(int threadCount) {
            this.threadCount = threadCount;
            return this;
        }

        public Builder setTimeoutMs(long timeoutMs) {
            this.timeoutMs = timeoutMs;
            return this;
        }

        public Builder setBatchSize(int batchSize) {
            this.batchSize = batchSize;
            return this;
        }

        public Builder setStrategy(Strategy strategy) {
            this.strategy = (strategy == Strategy.DEFAULT) ? Strategy.TIMEOUT : strategy;
            return this;
        }

        private int threadCount = 1;
        private long timeoutMs = 100;
        private int batchSize = 10000;
        private Strategy strategy = Strategy.TIMEOUT;
    }

    private ImportOptions(int threadCount,
                          long timeoutMs,
                          int batchSize,
                          Strategy strategy) {
        this.threadCount = threadCount;
        this.timeoutMs = timeoutMs;
        this.batchSize = batchSize;
        this.strategy = strategy;
    }

    public static Builder builder() {
        return new Builder();
    }

    public int getThreadCount() {
        return this.threadCount;
    }

    public long getTimeoutMs() {
        return this.timeoutMs;
    }

    public int getBatchSize() {
        return this.batchSize;
    }

    public Strategy getStrategy() {
        return this.strategy;
    }

    public long getSliceWidth() {
        return 1048576L;
    }

    final private int threadCount;
    final private long timeoutMs;
    final private int batchSize;
    final private Strategy strategy;
}
