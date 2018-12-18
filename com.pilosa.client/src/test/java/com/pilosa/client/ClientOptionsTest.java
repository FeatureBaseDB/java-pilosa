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

import org.apache.http.ssl.SSLContextBuilder;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import javax.net.ssl.SSLContext;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;

import static org.junit.Assert.assertEquals;

@Category(UnitTest.class)
public class ClientOptionsTest {
    @Test
    public void testCreateDefaults() {
        ClientOptions options = ClientOptions.builder().build();
        assertEquals(10, options.getConnectionPoolSizePerRoute());
        assertEquals(100, options.getConnectionPoolTotalSize());
        assertEquals(30000, options.getConnectTimeout());
        assertEquals(300000, options.getSocketTimeout());
        assertEquals(3, options.getRetryCount());
    }

    @Test
    public void testCreate() throws KeyManagementException, NoSuchAlgorithmException {
        SSLContext sslContext = new SSLContextBuilder().build();
        ClientOptions options = ClientOptions.builder()
                .setConnectionPoolSizePerRoute(2)
                .setConnectionPoolTotalSize(50)
                .setConnectTimeout(100)
                .setSocketTimeout(1000)
                .setRetryCount(5)
                .setSslContext(sslContext)
                .setSkipVersionCheck()
                .setLegacyMode(true)
                .setShardWidth(1024)
                .build();
        assertEquals(2, options.getConnectionPoolSizePerRoute());
        assertEquals(50, options.getConnectionPoolTotalSize());
        assertEquals(100, options.getConnectTimeout());
        assertEquals(1000, options.getSocketTimeout());
        assertEquals(5, options.getRetryCount());
        assertEquals(sslContext, options.getSslContext());
        assertEquals(1024, options.getShardWidth());
    }
}
