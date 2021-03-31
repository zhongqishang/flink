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

package org.apache.flink.connector.jdbc.internal.connection;

import org.apache.flink.connector.jdbc.internal.options.JdbcLookupOptions;
import org.apache.flink.connector.jdbc.internal.options.JdbcOptions;
import org.apache.flink.connector.jdbc.statement.FieldNamedPreparedStatement;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.sql.SQLException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

/** Connection Manager to manage {@link JDBCConnection}. */
public class ConnectionManager implements Serializable {

    private static final Logger LOG = LoggerFactory.getLogger(ConnectionManager.class);
    private static final long serialVersionUID = 1L;

    private final String query;
    private final String[] keyNames;

    private final JdbcOptions jdbcOptions;
    private final JdbcLookupOptions lookupOptions;
    private transient BlockingQueue<JDBCConnection> jdbcConnectionPool;

    public ConnectionManager(
            JdbcOptions jdbcOptions,
            JdbcLookupOptions lookupOptions,
            String[] keyNames,
            String query) {
        this.jdbcOptions = jdbcOptions;
        this.lookupOptions = lookupOptions;
        this.keyNames = keyNames;
        this.query = query;
    }

    /**
     * Init connection pool.
     *
     * @throws SQLException
     * @throws ClassNotFoundException
     */
    public void initPool() throws SQLException, ClassNotFoundException {
        int poolSize = lookupOptions.getThreadPoolSize();

        LOG.info("Init connection pool and pool size is " + poolSize);
        if (null == jdbcConnectionPool) {
            jdbcConnectionPool = new ArrayBlockingQueue(poolSize);
        }

        for (int i = 0; i < poolSize; i++) {
            jdbcConnectionPool.add(createJDBCConnection());
        }
    }

    private JDBCConnection createJDBCConnection() throws SQLException, ClassNotFoundException {

        JdbcConnectionProvider connectionProvider = new SimpleJdbcConnectionProvider(jdbcOptions);

        FieldNamedPreparedStatement statement =
                FieldNamedPreparedStatement.prepareStatement(
                        connectionProvider.getOrEstablishConnection(), query, keyNames);

        return new JDBCConnection(connectionProvider, statement);
    }

    /**
     * Acquire JDBCConnection. If no available, it will be blocked here.
     *
     * @return JDBCConnection.
     */
    public JDBCConnection acquireJDBCConnection() throws InterruptedException {
        return jdbcConnectionPool.take();
    }

    /**
     * Release JDBCConnection when finish lookup.
     *
     * @param jdbcConnection
     */
    public void releaseJDBCConnection(JDBCConnection jdbcConnection) {
        jdbcConnectionPool.add(jdbcConnection);
    }

    public void close() throws SQLException {
        for (int i = 0; i < lookupOptions.getMaxPoolSize(); i++) {
            JDBCConnection jdbcConnection = jdbcConnectionPool.poll();
            if (null != jdbcConnection) {
                jdbcConnection.close();
            }
        }
    }
}
