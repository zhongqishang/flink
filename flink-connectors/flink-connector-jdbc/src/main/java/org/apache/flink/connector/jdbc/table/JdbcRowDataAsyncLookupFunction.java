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

import org.apache.flink.annotation.Internal;
import org.apache.flink.connector.jdbc.dialect.JdbcDialect;
import org.apache.flink.connector.jdbc.dialect.JdbcDialects;
import org.apache.flink.connector.jdbc.internal.connection.ConnectionManager;
import org.apache.flink.connector.jdbc.internal.connection.JDBCConnection;
import org.apache.flink.connector.jdbc.internal.connection.JdbcConnectionProvider;
import org.apache.flink.connector.jdbc.internal.converter.JdbcRowConverter;
import org.apache.flink.connector.jdbc.internal.options.JdbcLookupOptions;
import org.apache.flink.connector.jdbc.internal.options.JdbcOptions;
import org.apache.flink.connector.jdbc.statement.FieldNamedPreparedStatement;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.AsyncTableFunction;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import org.apache.flink.shaded.guava18.com.google.common.cache.Cache;
import org.apache.flink.shaded.guava18.com.google.common.cache.CacheBuilder;
import org.apache.flink.shaded.guava18.com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/** A lookup function for {@link JdbcDynamicTableSource}. */
@Internal
public class JdbcRowDataAsyncLookupFunction extends AsyncTableFunction<RowData> {

    private static final Logger LOG = LoggerFactory.getLogger(JdbcRowDataAsyncLookupFunction.class);
    private static final long serialVersionUID = 1L;

    private final String query;
    private final DataType[] keyTypes;
    private final String[] keyNames;
    private final long cacheMaxSize;
    private final long cacheExpireMs;
    private final int maxRetryTimes;
    private final JdbcDialect jdbcDialect;
    private final JdbcRowConverter jdbcRowConverter;
    private final JdbcRowConverter lookupKeyRowConverter;

    private ConnectionManager connectionManager;

    private final int threadPoolSize;
    private transient ExecutorService executorService;
    private transient Cache<RowData, List<RowData>> cache;

    public JdbcRowDataAsyncLookupFunction(
            JdbcOptions options,
            JdbcLookupOptions lookupOptions,
            String[] fieldNames,
            DataType[] fieldTypes,
            String[] keyNames,
            RowType rowType) {
        checkNotNull(options, "No JdbcOptions supplied.");
        checkNotNull(fieldNames, "No fieldNames supplied.");
        checkNotNull(fieldTypes, "No fieldTypes supplied.");
        checkNotNull(keyNames, "No keyNames supplied.");
        this.keyNames = keyNames;
        List<String> nameList = Arrays.asList(fieldNames);
        this.keyTypes =
                Arrays.stream(keyNames)
                        .map(
                                s -> {
                                    checkArgument(
                                            nameList.contains(s),
                                            "keyName %s can't find in fieldNames %s.",
                                            s,
                                            nameList);
                                    return fieldTypes[nameList.indexOf(s)];
                                })
                        .toArray(DataType[]::new);
        this.cacheMaxSize = lookupOptions.getCacheMaxSize();
        this.cacheExpireMs = lookupOptions.getCacheExpireMs();
        this.maxRetryTimes = lookupOptions.getMaxRetryTimes();
        this.query =
                options.getDialect()
                        .getSelectFromStatement(options.getTableName(), fieldNames, keyNames);
        String dbURL = options.getDbURL();
        this.jdbcDialect =
                JdbcDialects.get(dbURL)
                        .orElseThrow(
                                () ->
                                        new UnsupportedOperationException(
                                                String.format("Unknown dbUrl:%s", dbURL)));
        this.jdbcRowConverter = jdbcDialect.getRowConverter(rowType);
        this.lookupKeyRowConverter =
                jdbcDialect.getRowConverter(
                        RowType.of(
                                Arrays.stream(keyTypes)
                                        .map(DataType::getLogicalType)
                                        .toArray(LogicalType[]::new)));
        threadPoolSize = lookupOptions.getThreadPoolSize();

        connectionManager = new ConnectionManager(options, lookupOptions, keyNames, query);
    }

    @Override
    public void open(FunctionContext context) throws Exception {
        // Create lru memory cache
        this.cache =
                cacheMaxSize == -1 || cacheExpireMs == -1
                        ? null
                        : CacheBuilder.newBuilder()
                                .expireAfterWrite(cacheExpireMs, TimeUnit.MILLISECONDS)
                                .maximumSize(cacheMaxSize)
                                .build();
        // Create async lookup thread pool
        ThreadFactory threadFactory =
                new ThreadFactoryBuilder()
                        .setDaemon(true)
                        .setNameFormat("jdbc-aysnc-lookup-worker")
                        .build();
        executorService = Executors.newFixedThreadPool(threadPoolSize, threadFactory);
        connectionManager.initPool();
    }

    /**
     * The invoke entry point of lookup function.
     *
     * @param future The result or exception is returned.
     * @param keys the lookup keys.
     */
    public void eval(CompletableFuture<Collection<RowData>> future, Object... keys) {
        RowData keyRow = GenericRowData.of(keys);
        if (cache != null) {
            List<RowData> cachedRows = cache.getIfPresent(keyRow);
            if (cachedRows != null) {
                future.complete(cachedRows);
                return;
            }
        }

        CompletableFuture.runAsync(
                new Runnable() {
                    @Override
                    public void run() {
                        JDBCConnection connection = null;
                        try {
                            connection = connectionManager.acquireJDBCConnection();
                            executeAsyncLookup(future, keyRow, connection);
                        } catch (InterruptedException e) {
                            future.completeExceptionally(
                                    new RuntimeException(
                                            "Execution of JDBC get connection Interrupted.", e));
                        } finally {
                            if (connection != null) {
                                connectionManager.releaseJDBCConnection(connection);
                            }
                        }
                    }
                },
                executorService);
    }

    /**
     * Execute async query for JDBCAsyncLookupFunction.
     *
     * @param future The result or exception is returned.
     * @param connection JDBCConnection accesses to remote databases.
     */
    private void executeAsyncLookup(
            CompletableFuture<Collection<RowData>> future,
            RowData keyRow,
            JDBCConnection connection) {

        JdbcConnectionProvider connectionProvider = connection.getConnectionProvider();
        FieldNamedPreparedStatement statement = connection.getStatement();

        for (int retry = 0; retry <= maxRetryTimes; retry++) {
            try {
                statement = lookupKeyRowConverter.toExternal(keyRow, statement);
                try (ResultSet resultSet = statement.executeQuery()) {
                    ArrayList<RowData> rows = new ArrayList<>();
                    while (resultSet.next()) {
                        RowData row = jdbcRowConverter.toInternal(resultSet);
                        rows.add(row);
                    }
                    if (cache != null) {
                        rows.trimToSize();
                        cache.put(keyRow, rows);
                    }
                    future.complete(rows);
                }
                break;
            } catch (SQLException e) {
                LOG.error(String.format("JDBC executeBatch error, retry times = %d", retry), e);
                if (retry >= maxRetryTimes) {
                    future.completeExceptionally(
                            new RuntimeException("Execution of JDBC statement failed.", e));
                }

                try {
                    if (!connectionProvider.isConnectionValid()) {
                        statement.close();
                        connectionProvider.closeConnection();
                        connection = connectionManager.acquireJDBCConnection();
                        connectionProvider = connection.getConnectionProvider();
                        statement = connection.getStatement();
                    }
                } catch (SQLException excpetion) {
                    LOG.error(
                            "JDBC connection is not valid, and reestablish connection failed",
                            excpetion);
                    future.completeExceptionally(
                            new RuntimeException("Reestablish JDBC connection failed.", excpetion));
                } catch (InterruptedException interruptedException) {
                    future.completeExceptionally(
                            new RuntimeException(
                                    "Execution of JDBC get connection Interrupted.", e));
                }

                try {
                    Thread.sleep(1000 * retry);
                } catch (InterruptedException e1) {
                    future.completeExceptionally(
                            new RuntimeException(
                                    String.format(
                                            "JDBC asyncLookup error, retry times = %d", retry),
                                    e1));
                }
            }
        }
    }

    @Override
    public void close() {
        if (cache != null) {
            cache.cleanUp();
            cache = null;
        }
        if (connectionManager != null) {
            try {
                connectionManager.close();
            } catch (SQLException e) {
                LOG.info("JDBC statement could not be closed: " + e.getMessage());
            } finally {
                connectionManager = null;
            }
        }
    }
}
