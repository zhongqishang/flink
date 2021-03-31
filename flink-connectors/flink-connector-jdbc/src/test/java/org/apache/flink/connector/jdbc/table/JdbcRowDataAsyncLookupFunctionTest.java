/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.jdbc.table;

import org.apache.flink.connector.jdbc.internal.options.JdbcLookupOptions;
import org.apache.flink.connector.jdbc.internal.options.JdbcOptions;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;

import static org.apache.flink.connector.jdbc.JdbcTestFixture.DERBY_EBOOKSHOP_DB;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/** Test suite for {@link JdbcRowDataAsyncLookupFunction}. */
public class JdbcRowDataAsyncLookupFunctionTest extends JdbcLookupTestBase {

    private static String[] fieldNames = new String[] {"id1", "id2", "comment1", "comment2"};
    private static DataType[] fieldDataTypes =
            new DataType[] {
                DataTypes.INT(), DataTypes.STRING(), DataTypes.STRING(), DataTypes.STRING()
            };

    private static String[] lookupKeys = new String[] {"id1", "id2"};

    @Test
    public void testEval() throws Exception {

        JdbcRowDataAsyncLookupFunction lookupFunction = buildRowDataLookupFunction();

        lookupFunction.open(null);

        final List<String> result = new ArrayList<>();
        List<Object[]> keys = new ArrayList<>();
        keys.add(new Object[] {1, StringData.fromString("1")});
        keys.add(new Object[] {2, StringData.fromString("3")});
        CountDownLatch latch = new CountDownLatch(keys.size());

        for (Object[] rowkey : keys) {
            CompletableFuture<Collection<RowData>> future = new CompletableFuture<>();
            lookupFunction.eval(future, rowkey);
            future.whenComplete(
                    (rs, t) -> {
                        synchronized (result) {
                            if (rs.isEmpty()) {
                                result.add(rowkey + ": null");
                            } else {
                                rs.forEach(row -> result.add(row.toString()));
                            }
                        }
                        latch.countDown();
                    });
        }
        // this verifies lookup calls are async
        assertTrue(result.size() < keys.size());
        latch.await();
        lookupFunction.close();
        List<String> sortResult =
                Lists.newArrayList(result).stream().sorted().collect(Collectors.toList());
        List<String> expected = new ArrayList<>();
        expected.add("+I(1,1,11-c1-v1,11-c2-v1)");
        expected.add("+I(1,1,11-c1-v2,11-c2-v2)");
        expected.add("+I(2,3,null,23-c2)");
        assertEquals(expected, sortResult);
    }

    private JdbcRowDataAsyncLookupFunction buildRowDataLookupFunction() {
        JdbcOptions jdbcOptions =
                JdbcOptions.builder()
                        .setDriverName(DERBY_EBOOKSHOP_DB.getDriverClass())
                        .setDBUrl(DB_URL)
                        .setTableName(LOOKUP_TABLE)
                        .build();

        JdbcLookupOptions lookupOptions = JdbcLookupOptions.builder().build();

        RowType rowType =
                RowType.of(
                        Arrays.stream(fieldDataTypes)
                                .map(DataType::getLogicalType)
                                .toArray(LogicalType[]::new),
                        fieldNames);

        JdbcRowDataAsyncLookupFunction lookupFunction =
                new JdbcRowDataAsyncLookupFunction(
                        jdbcOptions,
                        lookupOptions,
                        fieldNames,
                        fieldDataTypes,
                        lookupKeys,
                        rowType);

        return lookupFunction;
    }
}
