package com.ning.metrics.goodwill.access;

import com.ning.http.client.AsyncCompletionHandler;
import com.ning.http.client.AsyncHttpClient;
import com.ning.http.client.Response;
import org.apache.log4j.Logger;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.concurrent.atomic.AtomicReference;

public class GoodwillAccessor
{
    private static final Logger log = Logger.getLogger(GoodwillAccessor.class);

    private final String host;
    private final int port;
    private String url;

    private final AsyncHttpClient client = new AsyncHttpClient();


    final AtomicReference<ThriftType> thriftFound = new AtomicReference<ThriftType>(null);

    public GoodwillAccessor(String host, int port)
    {
        this.host = host;
        this.port = port;

        this.url = String.format("http://%s:%d/registrar", host, port);
    }

    public void getSchema(String schemaName)
    {
        try {
            client.prepareGet(String.format("%s/%s", url, schemaName)).addHeader("Accept", "application/json").execute(new AsyncCompletionHandler<Response>()
            {
                @Override
                public Response onCompleted(Response response) throws Exception
                {
                    BufferedReader reader = new BufferedReader(new InputStreamReader(response.getResponseBodyAsStream()));
                    ObjectMapper mapper = new ObjectMapper();

                    thriftFound.set(mapper.readValue(reader, ThriftType.class));

                    reader.close();

                    return response;
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
        }
    }

    public ThriftType getThrift()
    {
        return thriftFound.get();
    }
}