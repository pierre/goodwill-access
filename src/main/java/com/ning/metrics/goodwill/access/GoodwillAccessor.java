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

import com.ning.http.client.AsyncCompletionHandler;
import com.ning.http.client.AsyncHttpClient;
import com.ning.http.client.AsyncHttpClientConfig;
import com.ning.http.client.Response;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.Future;

public class GoodwillAccessor extends Accessor
{
    protected static final ObjectMapper mapper = new ObjectMapper();
    protected AsyncHttpClient client;

    public GoodwillAccessor(final String host, final int port)
    {
        super(host, port);
        client = createHttpClient();
    }

    /**
     * Get the Goodwill schema associated to a schema name.
     * </p>
     * Typical invocation:
     * <pre>
     * try {
     *   GoodwillSchema type = accessor.getSchema("test").get();
     * ...
     * }
     * catch (Exception e) {
     *   // Connection exception? Goodwill server down?
     * }
     * </pre>
     *
     * @param schemaName schema name to lookup
     * @return a future on the Schema
     */
    public Future<GoodwillSchema> getSchema(final String schemaName)
    {
        try {
            return client.prepareGet(String.format("%s/%s", url, schemaName)).addHeader("Accept", "application/json").execute(new AsyncCompletionHandler<GoodwillSchema>()
            {
                @Override
                public GoodwillSchema onCompleted(final Response response) throws Exception
                {
                    if (response.getStatusCode() != 200) {
                        return null;
                    }

                    final InputStream in = response.getResponseBodyAsStream();
                    try {
                        return mapper.readValue(in, GoodwillSchema.class);
                    }
                    finally {
                        closeStream(in);
                    }
                }

                @Override
                public void onThrowable(final Throwable t)
                {
                    log.warn("Got exception looking up the schema", t);
                }
            });
        }
        catch (IOException e) {
            log.warn("Got exception looking up the schema", e);
            return null;
        }
    }

    /**
     * Get all schemata.
     * <p/>
     * Use schemata, instead of schemas, which is closer to the original σχήματα.
     *
     * @return a future on a list of Schema
     * @see #getSchema(String) for an invocation example
     */
    public Future<List<GoodwillSchema>> getSchemata()
    {
        try {
            return client.prepareGet(url).addHeader("Accept", "application/json").execute(new AsyncCompletionHandler<List<GoodwillSchema>>()
            {
                @Override
                public List<GoodwillSchema> onCompleted(final Response response) throws Exception
                {
                    if (response.getStatusCode() != 200) {
                        return null;
                    }

                    InputStream in = response.getResponseBodyAsStream();
                    try {
                        final HashMap<String, List<GoodwillSchema>> map = mapper.readValue(in,
                            new TypeReference<HashMap<String, List<GoodwillSchema>>>()
                            {
                            });
                        return map.get("types");
                    }
                    finally {
                        closeStream(in);
                    }
                }

                @Override
                public void onThrowable(final Throwable t)
                {
                    log.warn("Got exception looking up the schema list", t);
                }
            });
        }
        catch (IOException e) {
            log.warn("Got exception looking up the schema list", e);
            return null;
        }
    }

    // note: if called from base-class constructor, couldn't sub-class; hence just make static
    private static AsyncHttpClient createHttpClient()
    {
        // Don't limit the number of connections per host
        // See https://github.com/ning/async-http-client/issues/issue/28
        final AsyncHttpClientConfig.Builder builder = new AsyncHttpClientConfig.Builder();
        builder.setMaximumConnectionsPerHost(-1);
        return new AsyncHttpClient(builder.build());
    }

    /**
     * Close the underlying http client
     */
    public synchronized void close()
    {
        client.close();
    }

    protected final void closeStream(final InputStream in)
    {
        if (in != null) {
            try {
                in.close();
            }
            catch (IOException e) {
                log.warn("Failed to close http-client - provided InputStream: {}", e.getLocalizedMessage());
            }
        }
    }
}
