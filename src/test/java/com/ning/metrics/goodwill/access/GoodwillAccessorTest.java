package com.ning.metrics.goodwill.access;

import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.SerializationConfig;
import org.testng.annotations.Test;

public class GoodwillAccessorTest
{
    @Test(enabled = false)
    public void testGetSchema() throws Exception
    {
        GoodwillAccessor accessor = new GoodwillAccessor("localhost", 8080);
        accessor.getSchema("Awesomeness");
        Thread.sleep(2000);
        ObjectMapper mapper = new ObjectMapper();
        mapper.configure(SerializationConfig.Feature.INDENT_OUTPUT, true);
        System.out.println(mapper.writeValueAsString(accessor.getThrift()));
    }
}
