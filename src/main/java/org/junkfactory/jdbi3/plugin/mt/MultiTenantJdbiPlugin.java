/*
 * Copyright 2018 Junk.Factory
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated
 * documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to
 * whom the Software is furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all copies or substantial portions of the
 * Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT
 * LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
 * WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */

package org.junkfactory.jdbi3.plugin.mt;

import org.jdbi.v3.core.spi.JdbiPlugin;
import org.junkfactory.jdbi3.plugin.mt.configuration.DatabaseConfiguration;
import org.junkfactory.jdbi3.plugin.mt.configuration.DatabaseConfigurationException;
import org.junkfactory.jdbi3.plugin.mt.provider.DatabaseConfigurationProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.function.Supplier;

class MultiTenantJdbiPlugin implements JdbiPlugin {

    private static final Logger logger = LoggerFactory.getLogger(MultiTenantJdbiPlugin.class);

    private final Supplier<String> currentTenantResolver;
    private final DatabaseConfigurationProvider databaseConfigurationProvider;

    public MultiTenantJdbiPlugin(Supplier<String> currentTenantResolver, DatabaseConfigurationProvider databaseConfigurationProvider) {
        this.currentTenantResolver = currentTenantResolver;
        this.databaseConfigurationProvider = databaseConfigurationProvider;
    }

    @Override
    public Connection customizeConnection(Connection conn) {
        switchTenantDatabaseIfNecessary(conn);
        return conn;
    }

    /**
     * Switch the tenant database. Only switches the database if there is more than 1 tenant
     * @param conn The JDBC {@link Connection}
     */
    protected void switchTenantDatabaseIfNecessary(Connection conn) {
        final String currentTenant = currentTenantResolver.get();
        final DatabaseConfiguration databaseConfiguration = databaseConfigurationProvider.get(currentTenant)
                .orElseThrow(() -> new DatabaseConfigurationException("Cannot find database configuration for tenant " + currentTenant));
        final int numTenants = databaseConfigurationProvider.getNumTenants();
        logger.debug("Customize connection, tenant={}, database={}, numTenants={}", currentTenant, databaseConfiguration, numTenants);
        //only customize connection only if number of tenants
        if (numTenants > 1) {
            try (Statement statement = conn.createStatement()) {
                statement.execute("USE " + databaseConfiguration.getDatabaseName());
            } catch (SQLException e) {
                logger.error("Failed to switch db for tenant={}, database={}", currentTenant, databaseConfiguration);
                throw new IllegalStateException("Failed to switch db for tenant " + currentTenant, e);
            }
        }
    }
}
