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

package io.crate.execution.dml;

import java.io.IOException;
import java.util.Map;
import java.util.function.Consumer;

import javax.annotation.Nullable;

import org.apache.lucene.document.DoublePoint;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.util.NumericUtils;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.mapper.NumberFieldMapper;

import io.crate.execution.dml.Indexer.ColumnConstraint;
import io.crate.execution.dml.Indexer.Synthetic;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.Reference;

public class DoubleIndexer implements ValueIndexer<Number> {

    private final Reference ref;
    private final FieldType fieldType;
    private final String name;

    public DoubleIndexer(Reference ref, @Nullable FieldType fieldType) {
        this.ref = ref;
        this.fieldType = fieldType == null ? NumberFieldMapper.FIELD_TYPE : fieldType;
        this.name = ref.column().fqn();
    }

    @Override
    public void indexValue(Number value,
                           XContentBuilder xcontentBuilder,
                           Consumer<? super IndexableField> addField,
                           Consumer<? super Reference> onDynamicColumn,
                           Map<ColumnIdent, Synthetic> synthetics,
                           Map<ColumnIdent, ColumnConstraint> toValidate) throws IOException {
        xcontentBuilder.value(value);
        double doubleValue = value.doubleValue();
        addField.accept(new DoublePoint(name, doubleValue));
        if (ref.hasDocValues()) {
            addField.accept(new SortedNumericDocValuesField(
                name,
                NumericUtils.doubleToSortableLong(doubleValue)));
        }
        if (fieldType.stored()) {
            addField.accept(new StoredField(name, doubleValue));
        }
    }
}
