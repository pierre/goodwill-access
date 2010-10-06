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

import org.apache.thrift.protocol.TType;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

/**
 * Describe a TField
 */
public class ThriftField
{
    private static final ObjectMapper mapper = new ObjectMapper();

    @JsonProperty
    private String name;
    public static final String JSON_THRIFT_FIELD_NAME = "name";

    @JsonProperty
    private byte type;
    public static final String JSON_THRIFT_FIELD_TYPE = "type";

    @JsonProperty
    private Integer position;
    public static final String JSON_THRIFT_FIELD_POSITION = "position";

    @JsonProperty
    private String description;
    public static final String JSON_THRIFT_FIELD_DESCRIPTION = "description";

    /*
     * When storing the thrifts in a database/datawarehouse, use these fields to describe
     * the associated sql type and length.
     * Given the large range of data types (Netezza, Oracle, MySQL, ...), no enforcement
     * is performed (plain Strings).
     */
    @JsonProperty
    private Sql sql;
    public static final String JSON_THRIFT_FIELD_SQL_KEY = "sql";
    public static final String JSON_THRIFT_FIELD_SQL_TYPE = "type";
    public static final String JSON_THRIFT_FIELD_SQL_LENGTH = "length";
    public static final String JSON_THRIFT_FIELD_SQL_SCALE = "scale";
    public static final String JSON_THRIFT_FIELD_SQL_PRECISION = "precision";


    /* Human readable representation of Thrift internal types */
    private static final String TTYPE_STRING = "string";
    private static final String TTYPE_I64 = "i64";
    private static final String TTYPE_I32 = "i32";
    private static final String TTYPE_I16 = "i16";
    private static final String TTYPE_BYTE = "i8";
    private static final String TTYPE_BOOL = "bool";
    private static final String TTYPE_DOUBLE = "double";

    /*
     * Default constructor for Jackson.
     */

    @SuppressWarnings("unused")
    public ThriftField()
    {
    }

    /*
     * Manual constructor, typically used by Goodwill stores.
     */

    public ThriftField(
        String name,
        String typeString,
        Integer position,
        String description,
        String sqlType,
        Integer sqlLength,
        Integer sqlScale,
        Integer sqlPrecision
    )
    {
        if (name == null) {
            throw new IllegalArgumentException("ThriftField name can't be null");
        }
        this.name = name;

        this.type = ttypeFromString(typeString);

        if (position == null) {
            throw new IllegalArgumentException("ThriftField position can't be null");
        }
        this.position = position;

        if ((sqlType == null || sqlType.equals("string")) && (sqlScale != null || sqlPrecision != null)) {
            throw new IllegalArgumentException("Strings cannot have a scale or precision");
        }

        // Optional fields
        sql = new Sql(sqlType, sqlLength, sqlScale, sqlPrecision);
        this.description = description;

    }

    public static ThriftField decode(
        String thriftItemJson
    ) throws IOException
    {
        return mapper.readValue(thriftItemJson, ThriftField.class);
    }

    /**
     * Lookup a TType associated with a human readable string
     *
     * @param type human readable string
     * @return the TType associated to the type
     */
    private byte ttypeFromString(String type)
    {
        if (type.equals(TTYPE_STRING)) {
            return TType.STRING;
        }
        else if (type.equals(TTYPE_I64)) {
            return TType.I64;
        }
        else if (type.equals(TTYPE_I32)) {
            return TType.I32;
        }
        else if (type.equals(TTYPE_I16)) {
            return TType.I16;
        }
        else if (type.equals(TTYPE_BYTE)) {
            return TType.BYTE;
        }
        else if (type.equals(TTYPE_BOOL)) {
            return TType.BOOL;
        }
        else if (type.equals(TTYPE_DOUBLE)) {
            return TType.DOUBLE;
        }
        else {
            throw new IllegalArgumentException(String.format("%s not a valid TType", type));
        }
    }

    /**
     * Return a human readable string representing a TType
     *
     * @param b TField type
     * @return human readable representation of the type b
     */
    private static String typeStringfromTType(byte b)
    {
        switch (b) {
            case TType.STRING:
                return TTYPE_STRING;
            case TType.I64:
                return TTYPE_I64;
            case TType.I32:
                return TTYPE_I32;
            case TType.I16:
                return TTYPE_I16;
            case TType.BYTE:
                return TTYPE_BYTE;
            case TType.BOOL:
                return TTYPE_BOOL;
            case TType.DOUBLE:
                return TTYPE_DOUBLE;
            default:
                throw new IllegalArgumentException(String.format("%d not a valid TType", b));
        }
    }

    public static class Sql
    {
        private String type;
        private Integer length;
        private Integer scale;
        private Integer precision;

        @JsonCreator
        public Sql(
            @JsonProperty("type") String type,
            @JsonProperty("length") Integer length,
            @JsonProperty("scale") Integer scale,
            @JsonProperty("precision") Integer precision)
        {
            this.type = type;
            this.length = length;
            this.scale = scale;
            this.precision = precision;
        }

        @SuppressWarnings("unused")
        public String getType()
        {
            return type;
        }

        @SuppressWarnings("unused")
        public Integer getLength()
        {
            return length;
        }

        @SuppressWarnings("unused")
        public Integer getScale()
        {
            return scale;
        }

        @SuppressWarnings("unused")
        public Integer getPrecision()
        {
            return precision;
        }
    }

    public String getName()
    {
        return name;
    }

    public String getType()
    {
        return ThriftField.typeStringfromTType(type);
    }

    /**
     * Set the Thrift type from a human readable String representation.
     * This is really to help Jackson set the type correctly from JSON.
     *
     * @param type Human readable String represenation of the Thrift Type.
     */
    @SuppressWarnings("unused")
    private void setType(String type)
    {
        this.type = ttypeFromString(type);
    }

    /**
     * Return the field position of the described ThriftField in the associated
     * ThriftType.
     *
     * @return the field position
     */
    public Integer getPosition()
    {
        return position;
    }

    public Sql getSql()
    {
        return sql;
    }

    public String getDescription()
    {
        return description;
    }

    @Override
    public String toString()
    {
        try {
            return toJSON().toString();
        }
        catch (JsonGenerationException e) {
            return "ThriftField{" +
                JSON_THRIFT_FIELD_NAME + "='" + name + '\'' +
                ", " + JSON_THRIFT_FIELD_TYPE + "='" + ThriftField.typeStringfromTType(type) + '\'' +
                ", " + JSON_THRIFT_FIELD_POSITION + "=" + position +
                ", " + JSON_THRIFT_FIELD_SQL_TYPE + "='" + sql.type + '\'' +
                ", " + JSON_THRIFT_FIELD_SQL_LENGTH + "=" + sql.length +
                ", " + JSON_THRIFT_FIELD_SQL_SCALE + "=" + sql.scale +
                ", " + JSON_THRIFT_FIELD_SQL_PRECISION + "=" + sql.precision +
                ", " + JSON_THRIFT_FIELD_DESCRIPTION + "=" + description +
                '}';
        }
        catch (IOException e) {
            return "ThriftField{" +
                JSON_THRIFT_FIELD_NAME + "='" + name + '\'' +
                ", " + JSON_THRIFT_FIELD_TYPE + "='" + ThriftField.typeStringfromTType(type) + '\'' +
                ", " + JSON_THRIFT_FIELD_POSITION + "=" + position +
                ", " + JSON_THRIFT_FIELD_SQL_TYPE + "='" + sql.type + '\'' +
                ", " + JSON_THRIFT_FIELD_SQL_LENGTH + "=" + sql.length +
                ", " + JSON_THRIFT_FIELD_SQL_SCALE + "=" + sql.scale +
                ", " + JSON_THRIFT_FIELD_SQL_PRECISION + "=" + sql.precision +
                ", " + JSON_THRIFT_FIELD_DESCRIPTION + "=" + description +
                '}';
        }
    }

    /**
     * Create a JSON representation of the ThriftField. It will always contain
     * the name, type and position. Description and SQL attributes are however
     * optional.
     *
     * @return JSONObject containing all fields
     * @throws IOException             if a serialization exception occurs
     * @throws JsonGenerationException if a serialization exception occurs
     */
    public ByteArrayOutputStream toJSON() throws IOException
    {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        mapper.writeValue(out, this);
        return out;
    }

    /**
     * Pretty print the SQL type.
     * TODO: add layer of abstraction, too Netezza specific
     *
     * @return a human readable representation of the SQL type
     */
    @JsonIgnore
    @SuppressWarnings("unused")
    public String getFullSQLType()
    {
        String fullSQLType = null;

        if (sql.type == null) {
            return null;
        }
        else if ((sql.type.equals("decimal")) || sql.type.equals("numeric")) {
            if (sql.precision != null) {
                if (sql.scale != null) {
                    fullSQLType = sql.type + "(" + sql.precision + ", " + sql.scale + ")";
                }
                else {
                    fullSQLType = sql.type + "(" + sql.precision + ")";
                }
            }
        }
        else {
            if (sql.type.equals("nvarchar") || sql.type.equals("varchar")) {
                if (sql.length != null) {
                    fullSQLType = sql.type + "(" + sql.length + ")";
                }
            }
        }

        if (fullSQLType == null) {
            fullSQLType = sql.type;
        }

        return fullSQLType;
    }
}
