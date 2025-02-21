/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial agreement.
 */

package io.crate.expression.reference.doc.lucene;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.Nullable;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentParser.Token;
import org.elasticsearch.common.xcontent.XContentType;

import io.crate.metadata.ColumnIdent;
import io.crate.metadata.doc.DocSysColumns;
import io.crate.sql.tree.BitString;
import io.crate.types.ArrayType;
import io.crate.types.BitStringType;
import io.crate.types.BooleanType;
import io.crate.types.ByteType;
import io.crate.types.DataType;
import io.crate.types.DoubleType;
import io.crate.types.FloatType;
import io.crate.types.GeoPointType;
import io.crate.types.IntegerType;
import io.crate.types.LongType;
import io.crate.types.ObjectType;
import io.crate.types.ShortType;
import io.crate.types.TimestampType;

import static org.elasticsearch.common.xcontent.XContentParser.Token.VALUE_NULL;

public final class SourceParser {

    private final Map<String, Object> requiredColumns = new HashMap<>();

    public SourceParser() {
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    public void register(ColumnIdent docColumn, DataType<?> type) {
        assert docColumn.name().equals(DocSysColumns.DOC.name()) && docColumn.path().size() > 0
            : "All columns registered for sourceParser must start with _doc";

        List<String> path = docColumn.path();
        if (path.size() == 1) {
            requiredColumns.put(docColumn.path().get(0), type);
        } else {
            Map<String, Object> columns = requiredColumns;
            for (int i = 0; i < path.size(); i++) {
                String part = path.get(i);
                if (i + 1 == path.size()) {
                    columns.put(part, type);
                } else {
                    Object object = columns.get(part);
                    if (object instanceof Map) {
                        columns = (Map) object;
                    } else if (object instanceof DataType) {
                        break;
                    } else {
                        HashMap<String, Object> children = new HashMap<String, Object>();
                        columns.put(part, children);
                        columns = children;
                    }
                }
            }
        }
    }

    public Map<String, Object> parse(BytesReference bytes) {
        try (InputStream inputStream = XContentHelper.getUncompressedInputStream(bytes)) {
            XContentParser parser = XContentType.JSON.xContent().createParser(
                NamedXContentRegistry.EMPTY,
                DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                inputStream
            );
            Token token = parser.currentToken();
            if (token == null) {
                token = parser.nextToken();
            }
            return parseObject(parser, null, requiredColumns, false);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static Object parseArray(XContentParser parser,
                                     @Nullable DataType<?> type,
                                     @Nullable Map<String, Object> requiredColumns) throws IOException {
        if (type instanceof GeoPointType) {
            return type.implicitCast(parser.list());
        } else {
            ArrayList<Object> values = new ArrayList<>();
            Token token = parser.nextToken();
            // Handles nested arrays
            // ex:
            //   CREATE TABLE test (
            //   "a" array(object as (
            //   "b" array(object as (
            //   "s" string
            //   )))));
            //   SELECT a['b'] from test; -- resolves to array(array(object))
            while (type instanceof ArrayType) {
                type = ((ArrayType<?>) type).innerType();
            }
            for (; token != null && token != XContentParser.Token.END_ARRAY; token = parser.nextToken()) {
                values.add(parseValue(parser, type, requiredColumns));
            }
            return values;
        }
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static Map<String, Object> parseObject(XContentParser parser,
                                                   @Nullable DataType<?> type,
                                                   @Nullable Map<String, Object> requiredColumns,
                                                   boolean includeUnknown) throws IOException {
        if (requiredColumns == null || requiredColumns.isEmpty()) {
            return type == null ? parser.map() : (Map) type.implicitCast(parser.map());
        } else {
            HashMap<String, Object> values = new HashMap<>();
            XContentParser.Token token = parser.nextToken(); // move past START_OBJECT;
            for (; token == XContentParser.Token.FIELD_NAME; token = parser.nextToken()) {
                String fieldName = parser.currentName();
                token = parser.nextToken(); // Move to the current field's value
                var required = requiredColumns.get(fieldName);
                if (required == null && !includeUnknown) {
                    parser.skipChildren();
                } else if (token == VALUE_NULL) {
                    // If object value is null, we can short-circuit to NULL and continue further with other fields.
                    // We should not call parseObject() as current object's innerTypes can interfere with sibling columns
                    // and in case of same names cause parsing errors. See https://github.com/crate/crate/issues/13372
                    values.put(fieldName, null);
                } else if (required instanceof ObjectType objectType) {
                    values.put(fieldName, parseObject(parser, objectType, (Map) objectType.innerTypes(), true));
                } else if (required instanceof DataType<?> dataType) {
                    values.put(fieldName, parseValue(parser, dataType, null));
                } else {
                    values.put(fieldName, parseValue(parser, null, (Map) required));
                }
            }
            return values;
        }
    }

    /**
     * Parsing should preferable happen based on a non-null given data type even if the value is a string
     * inside the source.
     * Non-string values could be stored as strings inside the _source because we do not sanitize
     * the input on COPY FROM.
     */
    private static Object parseValue(XContentParser parser,
                                     @Nullable DataType<?> type,
                                     @Nullable Map<String, Object> requiredColumns) throws IOException {
        return switch (parser.currentToken()) {
            case VALUE_NULL -> null;
            case START_ARRAY -> parseArray(parser, type, requiredColumns);
            case START_OBJECT -> parseObject(parser, type, requiredColumns, false);
            case VALUE_STRING -> type == null ? parser.text() : parseByType(parser, type);
            case VALUE_NUMBER -> type == null ? parser.numberValue() : parseByType(parser, type);
            case VALUE_BOOLEAN -> type == null ? parser.booleanValue() : parseByType(parser, type);
            case VALUE_EMBEDDED_OBJECT -> type == null ? parser.binaryValue() : parseByType(parser, type);
            default -> {
                throw new UnsupportedOperationException("Unsupported token encountered, expected a value, got " + parser.currentToken());
            }
        };
    }

    private static Object parseByType(XContentParser parser, DataType<?> type) throws IOException {
        assert type != null : "Type must no be null when parsing data type aware";

        // Type could be an array if traversed into an object array → unnest to get the inner type
        var elementType = ArrayType.unnest(type);
        return switch (elementType.id()) {
            case BooleanType.ID -> parser.booleanValue();
            case ByteType.ID -> (byte) parser.intValue();
            case ShortType.ID -> parser.shortValue(true);
            case IntegerType.ID -> parser.intValue();
            case LongType.ID -> parser.longValue();
            case TimestampType.ID_WITH_TZ -> parser.longValue();
            case TimestampType.ID_WITHOUT_TZ -> parser.longValue();
            case FloatType.ID -> parser.floatValue();
            case DoubleType.ID -> parser.doubleValue();
            case BitStringType.ID -> new BitString(
                BitSet.valueOf(parser.binaryValue()),
                ((BitStringType) elementType).length()
            );
            default -> parser.text();
        };
    }
}
