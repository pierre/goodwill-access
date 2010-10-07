package com.ning.metrics.goodwill.access;

import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.SerializationConfig;
import org.testng.annotations.Test;

import java.util.List;
import java.util.concurrent.Future;

public class GoodwillAccessorTest
{
    @Test(enabled = false)
    public void testGetSchema() throws Exception
    {
        GoodwillAccessor accessor = new GoodwillAccessor("localhost", 8080);

        ObjectMapper mapper = new ObjectMapper();
        mapper.configure(SerializationConfig.Feature.INDENT_OUTPUT, true);

        Future<List<ThriftType>> thrifts = accessor.getSchemata();
        System.out.println(String.format("All thrifts:\n%s", mapper.writeValueAsString(thrifts.get())));

        Future<ThriftType> thrift = accessor.getSchema("Awesomeness");
        System.out.println(String.format("Awesomeness thrift:\n%s", mapper.writeValueAsString(thrift.get())));
    }
}
