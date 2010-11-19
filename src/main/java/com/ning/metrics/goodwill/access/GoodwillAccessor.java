/*
 * Copyright 2010 Ning, Inc.
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
import org.apache.log4j.Logger;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.Future;

public class GoodwillAccessor
{
    private static final Logger log = Logger.getLogger(GoodwillAccessor.class);

    private final String host;
    private final int port;
    private String url;

    private AsyncHttpClient client;

    public GoodwillAccessor(String host, int port)
    {
        this.host = host;
        this.port = port;

        this.url = String.format("http://%s:%d/registrar", host, port);

        createHttpClient();
    }

    private void createHttpClient()
    {
        // Don't limit the number of connections per host
        // See https://github.com/ning/async-http-client/issues/issue/28
        AsyncHttpClientConfig.Builder builder = new AsyncHttpClientConfig.Builder();
        builder.setMaximumConnectionsPerHost(-1);
        client = new AsyncHttpClient(builder.build());
    }

    /**
     * Get the Goodwill schema associated to a schema name.
     * </p>
     * Typical invocation:
     * <pre>
     * {@code
     * try {
     * GoodwillSchema type = accessor.getSchema("test").get();
     * ...
     * }
     * catch (Exception e) {
     * // Connection exception? Goodwill server down?
     * }
     * }
     * </pre>
     *
     * @param schemaName schema name to lookup
     * @return a future on the Schema
     */
    public Future<GoodwillSchema> getSchema(String schemaName)
    {
        try {
            return client.prepareGet(String.format("%s/%s", url, schemaName)).addHeader("Accept", "application/json").execute(new AsyncCompletionHandler<GoodwillSchema>()
            {
                @Override
                public GoodwillSchema onCompleted(Response response) throws Exception
                {
                    if (response.getStatusCode() != 200) {
                        return null;
                    }

                    BufferedReader reader = new BufferedReader(new InputStreamReader(response.getResponseBodyAsStream()));
                    ObjectMapper mapper = new ObjectMapper();

                    GoodwillSchema thrift = mapper.readValue(reader, GoodwillSchema.class);

                    reader.close();

                    return thrift;
                }

                @Override
                public void onThrowable(Throwable t)
                {
                    log.warn(t);
                }
            });
        }
        catch (IOException e) {
            log.warn(String.format("Error getting Schema list from %s:%d (%s)", host, port, e.getLocalizedMessage()));
            return null;
        }
    }

    /**
     * Get all schemata.
     * <p/>
     * Use schemata, instead of schemas, which is closer to the original σχήματα.
     *
     * @return a future on a list of Schema
     * @see #getSchema for an invocation example
     */
    public Future<List<GoodwillSchema>> getSchemata()
    {
        try {
            return client.prepareGet(url).addHeader("Accept", "application/json").execute(new AsyncCompletionHandler<List<GoodwillSchema>>()
            {
                @Override
                public List<GoodwillSchema> onCompleted(Response response) throws Exception
                {
                    if (response.getStatusCode() != 200) {
                        return null;
                    }

                    BufferedReader reader = new BufferedReader(new InputStreamReader(response.getResponseBodyAsStream()));
                    ObjectMapper mapper = new ObjectMapper();

                    HashMap<String, List<GoodwillSchema>> map = mapper.readValue(
                        reader,
                        new TypeReference<HashMap<String, List<GoodwillSchema>>>()
                        {
                        });
                    List<GoodwillSchema> goodwillSchemata = map.get("types");

                    reader.close();

                    return goodwillSchemata;
                }

                @Override
                public void onThrowable(Throwable t)
                {
                    log.warn(t);
                }
            });
        }
        catch (IOException e) {
            log.warn(String.format("Error getting Schema list from %s:%d (%s)", host, port, e.getLocalizedMessage()));
            return null;
        }
    }
}
