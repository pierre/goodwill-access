package com.ning.metrics.goodwill.access;

import com.ning.http.client.AsyncCompletionHandler;
import com.ning.http.client.AsyncHttpClient;
import com.ning.http.client.Response;
import org.apache.log4j.Logger;
import org.codehaus.jackson.map.ObjectMapper;

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

    private final AsyncHttpClient client = new AsyncHttpClient();

    public GoodwillAccessor(String host, int port)
    {
        this.host = host;
        this.port = port;

        this.url = String.format("http://%s:%d/registrar", host, port);
    }

    /**
     * Get the Goodwill schema associated to a schema name.
     * </p>
     * Typical invocation:
     * <pre>
     * {@code
     * try {
     * ThriftType type = accessor.getSchemata().get();
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
    public Future<ThriftType> getSchema(String schemaName)
    {
        try {
            return client.prepareGet(String.format("%s/%s", url, schemaName)).addHeader("Accept", "application/json").execute(new AsyncCompletionHandler<ThriftType>()
            {
                @Override
                public ThriftType onCompleted(Response response) throws Exception
                {
                    if (response.getStatusCode() != 200) {
                        return null;
                    }

                    BufferedReader reader = new BufferedReader(new InputStreamReader(response.getResponseBodyAsStream()));
                    ObjectMapper mapper = new ObjectMapper();

                    ThriftType thrift = mapper.readValue(reader, ThriftType.class);

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
            log.warn(String.format("Error getting Schema list from %s:%d", host, port));
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
    public Future<List<ThriftType>> getSchemata()
    {
        try {
            return client.prepareGet(url).addHeader("Accept", "application/json").execute(new AsyncCompletionHandler<List<ThriftType>>()
            {
                @Override
                public List<ThriftType> onCompleted(Response response) throws Exception
                {
                    if (response.getStatusCode() != 200) {
                        return null;
                    }

                    BufferedReader reader = new BufferedReader(new InputStreamReader(response.getResponseBodyAsStream()));
                    ObjectMapper mapper = new ObjectMapper();

                    HashMap<String, List<ThriftType>> map = mapper.readValue(reader, HashMap.class);
                    List<ThriftType> thriftTypes = map.get("types");

                    reader.close();

                    return thriftTypes;
                }

                @Override
                public void onThrowable(Throwable t)
                {
                    log.warn(t);
                }
            });
        }
        catch (IOException e) {
            log.warn(String.format("Error getting Schema list from %s:%d", host, port));
            return null;
        }
    }
}
