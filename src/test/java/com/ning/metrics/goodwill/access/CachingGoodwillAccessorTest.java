/*
 * Copyright 2010-2011 Ning, Inc.
 *
 * Ning licenses this file to you under the Apache License, version 2.0
 * (the "License"); you may not use this file except in compliance with the
 * License.  You may obtain a copy of the License at:
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package com.ning.metrics.goodwill.access;

import org.codehaus.jackson.map.ObjectMapper;
import org.eclipse.jetty.server.HttpConnection;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Response;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.bio.SocketConnector;
import org.eclipse.jetty.util.thread.ExecutorThreadPool;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import javax.servlet.ServletException;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class CachingGoodwillAccessorTest
{
    private static final int CACHE_TIMEOUT_IN_SECONDS = 3; // 3 seconds
    private static final String SCHEMA_NAME = "Schema";

    private int port;
    private SocketConnector connector;
    private Server server;
    private Server errorServer;
    private final AtomicInteger serversHits = new AtomicInteger(0);
    private boolean jettyShouldBomb = false;

    @BeforeClass(alwaysRun = true)
    public void setUpGlobal() throws Exception
    {
        port = findFreePort();
        connector = new SocketConnector();
        connector.setHost("127.0.0.1");
        connector.setPort(port);

        server = new Server()
        {
            @Override
            public void handle(HttpConnection connection) throws IOException, ServletException
            {
                serversHits.incrementAndGet();
                final Request request = connection.getRequest();
                final Response response = connection.getResponse();

                if (jettyShouldBomb) {
                    response.setStatus(500);
                    request.setHandled(true);
                    return;
                }
                else {
                    response.setStatus(200);
                }

                // Create a dummy schema
                List<GoodwillSchemaField> items = new ArrayList<GoodwillSchemaField>();
                items.add(new GoodwillSchemaField(SCHEMA_NAME, "STRING", (short) 0, null, "string", null, null, null));
                GoodwillSchema schema = new GoodwillSchema(SCHEMA_NAME, items);

                // Create the GoodwillSchema store
                Map<String, List<GoodwillSchema>> payload = new HashMap<String, List<GoodwillSchema>>();
                List<GoodwillSchema> list = new ArrayList<GoodwillSchema>();
                list.add(schema);
                payload.put("types", list); // Hardcoded value in Goodwill

                // Serialize the store and return it
                ByteArrayOutputStream out = new ByteArrayOutputStream();
                (new ObjectMapper()).writeValue(out, payload);
                out.close();

                response.getWriter().print(out.toString());
                request.setHandled(true);
            }
        };
        server.setThreadPool(new ExecutorThreadPool());
        server.addConnector(connector);
        connector.start();
        server.start();
    }

    @AfterClass(alwaysRun = true)
    public void tearDownGlobal() throws Exception
    {
        server.stop();
        connector.stop();
    }


    private int findFreePort() throws IOException
    {
        ServerSocket socket = null;

        try {
            socket = new ServerSocket(0);

            return socket.getLocalPort();
        }
        finally {
            if (socket != null) {
                socket.close();
            }
        }
    }


    @Test(groups = "slow")
    public void testCachingRegistrar() throws Exception
    {
        CachingGoodwillAccessor accessor = new CachingGoodwillAccessor("127.0.0.1", port, CACHE_TIMEOUT_IN_SECONDS);

        // It will warm the cache on startup - give it some time
        Thread.sleep(CACHE_TIMEOUT_IN_SECONDS * 1000 / 2);
        Assert.assertEquals(serversHits.get(), 1);

        // The caching accessor will hit the server if the schema does not exist
        GoodwillSchema schema = accessor.getSchema("Doesn'tExist");
        Assert.assertNull(schema);
        Assert.assertEquals(serversHits.get(), 2);

        // Try again
        schema = accessor.getSchema("Doesn'tExist");
        Assert.assertNull(schema);
        Assert.assertEquals(serversHits.get(), 3);

        // Valid schema in the cache shouldn't trigger a hit
        schema = accessor.getSchema(SCHEMA_NAME);
        Assert.assertNotNull(schema);
        Assert.assertEquals(serversHits.get(), 3); // No hit!

        // Test backend thread
        Thread.sleep(CACHE_TIMEOUT_IN_SECONDS * 1000);
        Assert.assertEquals(serversHits.get(), 4); // Refresh

        // We shouldn't care if Goodwill is down
        jettyShouldBomb = true;

        // The good schema is cached
        schema = accessor.getSchema(SCHEMA_NAME);
        Assert.assertNotNull(schema);
        Assert.assertEquals(serversHits.get(), 4);

        // The bad schemas are not
        schema = accessor.getSchema("NewOne");
        Assert.assertNull(schema);
        Assert.assertEquals(serversHits.get(), 5);

        accessor.close();
    }
}
