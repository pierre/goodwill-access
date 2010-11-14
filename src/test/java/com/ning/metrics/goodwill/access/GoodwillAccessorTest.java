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

import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.SerializationConfig;
import org.testng.Assert;
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

        Future<List<GoodwillSchema>> thrifts = accessor.getSchemata();

        List<GoodwillSchema> schemata = thrifts.get();
        // Simple test to make sure we got actual GoodwillSchema, 0.0.5 has the following bug:
        // java.lang.ClassCastException: java.util.LinkedHashMap cannot be cast to com.ning.metrics.goodwill.access.GoodwillSchema
        Assert.assertTrue(schemata.get(0).getName().length() > 0);

        System.out.println(String.format("All thrifts:\n%s", mapper.writeValueAsString(schemata)));

        Future<GoodwillSchema> thrift = accessor.getSchema("Awesomeness");

        GoodwillSchema schema = thrift.get();
        Assert.assertTrue(schema.getName().length() > 0);

        System.out.println(String.format("Awesomeness thrift:\n%s", mapper.writeValueAsString(schema)));
    }

    @Test(enabled = true)
    public void testNullSchema() throws Exception
    {
        GoodwillAccessor accessor = new GoodwillAccessor("unexistent", 8080);

        try {
            accessor.getSchemata().get();
            Assert.assertTrue(false);
        }
        catch (Exception e) {
            Assert.assertTrue(true);
        }
    }
}
