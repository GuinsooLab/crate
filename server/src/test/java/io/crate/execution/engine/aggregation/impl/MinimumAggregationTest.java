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

package io.crate.execution.engine.aggregation.impl;

import static io.crate.testing.Asserts.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;

import org.junit.Test;

import io.crate.metadata.functions.Signature;
import io.crate.operation.aggregation.AggregationTestCase;
import io.crate.types.DataType;
import io.crate.types.DataTypes;

public class MinimumAggregationTest extends AggregationTestCase {

    private Object executeAggregation(DataType<?> argumentType, Object[][] data) throws Exception {
        return executeAggregation(
            Signature.aggregate(
                MinimumAggregation.NAME,
                argumentType.getTypeSignature(),
                argumentType.getTypeSignature()
            ),
            data,
            List.of()
        );
    }

    @Test
    public void test_function_implements_doc_values_aggregator_for_numeric_types() {
        for (var dataType : DataTypes.NUMERIC_PRIMITIVE_TYPES) {
            assertHasDocValueAggregator(MinimumAggregation.NAME, List.of(dataType));
        }
    }

    @Test
    public void testDouble() throws Exception {
        Object result = executeAggregation(DataTypes.DOUBLE, new Object[][]{{0.8d}, {0.3d}});
        assertThat(result).isEqualTo(0.3d);
    }

    @Test
    public void testFloat() throws Exception {
        Object result = executeAggregation(DataTypes.FLOAT, new Object[][]{{0.8f}, {0.3f}});
        assertThat(result).isEqualTo(0.3f);
    }

    @Test
    public void test_aggregate_double_zero() throws Exception {
        Object result = executeAggregation(DataTypes.DOUBLE, new Object[][]{{0.0}, {0.0}});
        assertThat(result).isEqualTo((0.0d));
    }

    @Test
    public void test_aggregate_float_zero() throws Exception {
        Object result = executeAggregation(DataTypes.FLOAT, new Object[][]{{0.0f}, {0.0f}});
        assertThat(result).isEqualTo((0.0f));
    }

    @Test
    public void testInteger() throws Exception {
        Object result = executeAggregation(DataTypes.INTEGER, new Object[][]{{8}, {3}});
        assertThat(result).isEqualTo(3);
    }

    @Test
    public void testLong() throws Exception {
        Object result = executeAggregation(DataTypes.LONG, new Object[][]{{8L}, {3L}});
        assertThat(result).isEqualTo(3L);
    }

    @Test
    public void testShort() throws Exception {
        Object result = executeAggregation(DataTypes.SHORT, new Object[][]{{(short) 8}, {(short) 3}});
        assertThat(result).isEqualTo((short) 3);
    }

    @Test
    public void test_min_with_byte_argument_type() throws Exception {
        assertThat(executeAggregation(DataTypes.BYTE, new Object[][]{{(byte) 1}, {(byte) 0}})).isEqualTo(((byte) 0));
    }

    @Test
    public void testString() throws Exception {
        Object result = executeAggregation(DataTypes.STRING, new Object[][]{{"Youri"}, {"Ruben"}});
        assertThat(result).isEqualTo("Ruben");
    }

    @Test
    public void testUnsupportedType() throws Exception {
        assertThatThrownBy(() -> executeAggregation(DataTypes.INTERVAL, new Object[][]{{new Object()}}))
            .isExactlyInstanceOf(UnsupportedOperationException.class)
            .hasMessageStartingWith("Unknown function: min(INPUT(0)), no overload found for matching argument types: (interval).");
        assertThatThrownBy(() -> executeAggregation(DataTypes.UNTYPED_OBJECT, new Object[][]{{new Object()}}))
            .isExactlyInstanceOf(UnsupportedOperationException.class)
            .hasMessageStartingWith("Unknown function: min(INPUT(0)), no overload found for matching argument types: (object).");
    }
}
