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

import com.google.common.collect.ImmutableMap;
import com.ning.metrics.serialization.schema.SchemaField;
import com.ning.metrics.serialization.schema.SchemaFieldType;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.annotate.JsonValue;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

/**
 * Describe a SchemaField in Goodwill.
 * This is basically the union of a SchemaField and extra metadata for the SQL sink.
 *
 * @see com.ning.metrics.serialization.schema.SchemaField
 */
public class GoodwillSchemaField
{
    private static final ObjectMapper mapper = new ObjectMapper();

    public static final String JSON_THRIFT_FIELD_NAME = "name";
    public static final String JSON_THRIFT_FIELD_TYPE = "type";
    public static final String JSON_THRIFT_FIELD_ID = "position";
    public static final String JSON_THRIFT_FIELD_DESCRIPTION = "description";

    /*
     * When storing the thrifts in a database/datawarehouse, use these fields to describe
     * the associated sql type and length.
     * Given the large range of data types (Netezza, Oracle, MySQL, ...), no enforcement
     * is performed (plain Strings).
     */
    public static final String JSON_THRIFT_FIELD_SQL_KEY = "sql";
    public static final String JSON_THRIFT_FIELD_SQL_TYPE = "type";
    public static final String JSON_THRIFT_FIELD_SQL_LENGTH = "length";
    public static final String JSON_THRIFT_FIELD_SQL_SCALE = "scale";
    public static final String JSON_THRIFT_FIELD_SQL_PRECISION = "precision";

    private final SchemaField schemaField;
    private final String description;
    private Sql sql;

    /**
     * Jackson constructor
     * <p/>
     * {
     * "name": "myField",
     * "type": "string",
     * "position": 1,
     * "description": "string",
     * "sql": {
     * "type": "nvarchar",
     * "length": 255,
     * "scale": null,
     * "precision": null
     * }
     *
     * @param name        Schema field name
     * @param type        Schema field type. This is not necessarily a Thrift type. @see SchemaFieldType
     * @param id          field position
     * @param description Short description of the field
     * @param sql         SQL object (used by the sink)
     */
    @JsonCreator
    @SuppressWarnings("unused")
    public GoodwillSchemaField(
        @JsonProperty(JSON_THRIFT_FIELD_NAME) final String name,
        @JsonProperty(JSON_THRIFT_FIELD_TYPE) final String type,
        @JsonProperty(JSON_THRIFT_FIELD_ID) final short id,
        @JsonProperty(JSON_THRIFT_FIELD_DESCRIPTION) final String description,
        @JsonProperty(JSON_THRIFT_FIELD_SQL_KEY) final Sql sql
    )
    {
        this(name, type, id, description, sql == null ? null : sql.getType(), sql == null ? null : sql.getLength(), sql == null ? null : sql.getScale(), sql == null ? null : sql.getPrecision());
    }

    /**
     * Manual constructor, typically used by Goodwill stores.
     *
     * @param name         Schema field name
     * @param type         Schema field type. This is not necessarily a Thrift type. @see SchemaFieldType
     * @param id           field position
     * @param description  Short description of the field
     * @param sqlType      SQL type (varchar, int, ...)
     * @param sqlLength    SQL type length
     * @param sqlScale     SQL type scale
     * @param sqlPrecision SQL type precision
     */
    public GoodwillSchemaField(
        final String name,
        final String type,
        final short id,
        final String description,
        final String sqlType,
        final Integer sqlLength,
        final Integer sqlScale,
        final Integer sqlPrecision
    )
    {
        if (name == null) {
            throw new IllegalArgumentException("GoodwillSchemaField name can't be null");
        }

        if ((sqlType == null || sqlType.equals("string")) && (sqlScale != null || sqlPrecision != null)) {
            throw new IllegalArgumentException("Strings cannot have a scale or precision");
        }

        this.schemaField = SchemaFieldType.createSchemaField(name, type, id);

        // Optional fields
        sql = new Sql(sqlType, sqlLength, sqlScale, sqlPrecision);
        this.description = description;
    }

    public GoodwillSchemaField(final SchemaField field)
    {
        this(field.getName(), field.getType().name(), field.getId(), null, null);
    }

    public static GoodwillSchemaField decode(final String thriftItemJson) throws IOException
    {
        return mapper.readValue(thriftItemJson, GoodwillSchemaField.class);
    }

    @JsonValue
    @SuppressWarnings({"unchecked", "unused"})
    public ImmutableMap toMap()
    {
        return new ImmutableMap.Builder()
            .put(JSON_THRIFT_FIELD_NAME, getName())
            .put(JSON_THRIFT_FIELD_TYPE, getType())
            .put(JSON_THRIFT_FIELD_ID, getId())
            .put(JSON_THRIFT_FIELD_DESCRIPTION, getDescription() == null ? "" : getDescription())
            .put(JSON_THRIFT_FIELD_SQL_KEY, getSql() == null ? "" : getSql())
            .build();
    }

    /**
     * Extra information for the SQL Sink
     */
    public static class Sql
    {
        private final String type;
        private final Integer length;
        private final Integer scale;
        private final Integer precision;

        @JsonCreator
        public Sql(
            @JsonProperty(JSON_THRIFT_FIELD_SQL_TYPE) final String type,
            @JsonProperty(JSON_THRIFT_FIELD_SQL_LENGTH) final Integer length,
            @JsonProperty(JSON_THRIFT_FIELD_SQL_SCALE) final Integer scale,
            @JsonProperty(JSON_THRIFT_FIELD_SQL_PRECISION) final Integer precision)
        {
            this.type = type;
            this.length = length;
            this.scale = scale;
            this.precision = precision;
        }


        @JsonValue
        @SuppressWarnings({"unchecked", "unused"})
        public ImmutableMap toMap()
        {
            return new ImmutableMap.Builder()
                .put(JSON_THRIFT_FIELD_SQL_TYPE, getType() == null ? "" : getType())
                .put(JSON_THRIFT_FIELD_SQL_LENGTH, getLength() == null ? "" : getLength())
                .put(JSON_THRIFT_FIELD_SQL_SCALE, getScale() == null ? "" : getScale())
                .put(JSON_THRIFT_FIELD_SQL_PRECISION, getPrecision() == null ? "" : getPrecision())
                .build();
        }

        public String getType()
        {
            return type;
        }

        public Integer getLength()
        {
            return length;
        }

        public Integer getScale()
        {
            return scale;
        }

        public Integer getPrecision()
        {
            return precision;
        }
    }

    public String getName()
    {
        return schemaField.getName();
    }

    public SchemaFieldType getType()
    {
        return schemaField.getType();
    }

    public short getId()
    {
        return schemaField.getId();
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
            return "GoodwillSchemaField{" +
                JSON_THRIFT_FIELD_NAME + "='" + getName() + '\'' +
                ", " + JSON_THRIFT_FIELD_TYPE + "='" + getType() + '\'' +
                ", " + JSON_THRIFT_FIELD_ID + "=" + getId() +
                ", " + JSON_THRIFT_FIELD_SQL_TYPE + "='" + sql.type + '\'' +
                ", " + JSON_THRIFT_FIELD_SQL_LENGTH + "=" + sql.length +
                ", " + JSON_THRIFT_FIELD_SQL_SCALE + "=" + sql.scale +
                ", " + JSON_THRIFT_FIELD_SQL_PRECISION + "=" + sql.precision +
                ", " + JSON_THRIFT_FIELD_DESCRIPTION + "=" + description +
                '}';
        }
        catch (IOException e) {
            return "GoodwillSchemaField{" +
                JSON_THRIFT_FIELD_NAME + "='" + getName() + '\'' +
                ", " + JSON_THRIFT_FIELD_TYPE + "='" + getType() + '\'' +
                ", " + JSON_THRIFT_FIELD_ID + "=" + getId() +
                ", " + JSON_THRIFT_FIELD_SQL_TYPE + "='" + sql.type + '\'' +
                ", " + JSON_THRIFT_FIELD_SQL_LENGTH + "=" + sql.length +
                ", " + JSON_THRIFT_FIELD_SQL_SCALE + "=" + sql.scale +
                ", " + JSON_THRIFT_FIELD_SQL_PRECISION + "=" + sql.precision +
                ", " + JSON_THRIFT_FIELD_DESCRIPTION + "=" + description +
                '}';
        }
    }

    /**
     * Create a JSON representation of the GoodwillSchemaField. It will always contain
     * the name, type and position. Description and SQL attributes are however
     * optional.
     *
     * @return JSONObject containing all fields
     * @throws IOException             if a serialization exception occurs
     * @throws JsonGenerationException if a serialization exception occurs
     */
    public ByteArrayOutputStream toJSON() throws IOException
    {
        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        mapper.writeValue(out, this);
        return out;
    }

    /**
     * Pretty print the SQL type.
     * TODO: add layer of abstraction, too Netezza specific
     *
     * @return a human readable representation of the SQL type
     */
    @SuppressWarnings("unused")
    public String getFullSQLType()
    {
        String fullSQLType = null;

        if (sql.type == null) {
            return null;
        }
        else if (sql.type.equals("decimal") || sql.type.equals("numeric")) {
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
