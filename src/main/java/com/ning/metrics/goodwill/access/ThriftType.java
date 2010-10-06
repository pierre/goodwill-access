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

import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;

public class ThriftType
{
    private static final ObjectMapper mapper = new ObjectMapper();

    @JsonProperty
    private String name;
    public static final String JSON_THRIFT_TYPE_NAME = "name";

    @JsonProperty
    private HashMap<Integer, ThriftField> thriftItems = new HashMap<Integer, ThriftField>();
    @SuppressWarnings("unused")
    public static final String JSON_THRIFT_TYPE_SCHEMA = "schema";

    @JsonProperty
    @SuppressWarnings("unused")
    private String sinkAddInfo;
    @SuppressWarnings("unused")
    public static final String JSON_THRIFT_TYPE_SINK_ADD_INFO = "sinkAddInfo";

    /*
     * Default constructor for Jackson.
     */

    public ThriftType()
    {
    }

    public ThriftType(String name, List<ThriftField> items)
    {
        this.name = name;
        setSchema(items);
    }

    public static ThriftType decode(
        String thriftJson
    ) throws IOException
    {
        return mapper.readValue(thriftJson, ThriftType.class);
    }

    /**
     * Add a field in the Thrift. The code does not enforce sanity w.r.t. field positions.
     *
     * @param thriftField field to add
     */
    public void addThriftField(ThriftField thriftField)
    {
        thriftItems.put(thriftField.getPosition(), thriftField);
    }

    public String getName()
    {
        return name;
    }

    /**
     * Given a list of ThriftField, build the internal hashmap of fields.
     * This is really to help Jackson decode a JSON correctly.
     *
     * @param items list of fields for the type
     */
    private void setSchema(List<ThriftField> items)
    {
        for (ThriftField field : items) {
            addThriftField(field);
        }
    }

    @SuppressWarnings("unused")
    public Collection<ThriftField> getSchema()
    {
        return thriftItems.values();
    }

    @SuppressWarnings("unused")
    public void setSinkAddInfo(String sinkAddInfo)
    {
        this.sinkAddInfo = sinkAddInfo;
    }

    /**
     * Given a position, return the field at that position.
     *
     * @param i position in the Thrift (start with 1)
     * @return the ThriftField object
     */
    public ThriftField getFieldByPosition(int i)
    {
        return thriftItems.get(i);
    }

    @Override
    public String toString()
    {
        try {
            return toJSON().toString();
        }
        catch (JsonGenerationException e) {
            return "ThriftType{" +
                JSON_THRIFT_TYPE_NAME + "='" + name + '\'' +
                ", thriftItems=" + thriftItems +
                '}';
        }
        catch (IOException e) {
            return "ThriftType{" +
                JSON_THRIFT_TYPE_NAME + "='" + name + '\'' +
                ", thriftItems=" + thriftItems +
                '}';
        }
    }

    public ByteArrayOutputStream toJSON() throws IOException
    {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        mapper.writeValue(out, this);
        return out;
    }
}