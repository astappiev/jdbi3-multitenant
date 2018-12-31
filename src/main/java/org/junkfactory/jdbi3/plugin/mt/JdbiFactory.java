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

import org.jdbi.v3.core.Jdbi;
import org.junkfactory.jdbi3.plugin.mt.configuration.DatabaseConfiguration;
import org.junkfactory.jdbi3.plugin.mt.configuration.DatabaseConfigurationException;
import org.junkfactory.jdbi3.plugin.mt.provider.DatabaseConfigurationProvider;

import javax.sql.DataSource;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;
import java.util.function.Supplier;

import static org.jdbi.v3.core.generic.internal.Preconditions.checkNotNull;

public class JdbiFactory {

    private static JdbiFactory INSTANCE;

    public static JdbiFactory getInstance() {
        return INSTANCE;
    }

    public static Initializer newInitializer() {
        return new Initializer();
    }

    private final Supplier<String> currentTenantResolver;
    private final Function<DatabaseConfiguration, DataSource> dataSourceProvider;
    private final DatabaseConfigurationProvider databaseConfigurationProvider;

    private final ConcurrentMap<String, Jdbi> jdbiTenantMap;

    private JdbiFactory(Initializer initializer) {
        currentTenantResolver = initializer.currentTenantResolver;
        dataSourceProvider = initializer.dataSourceProvider;
        databaseConfigurationProvider = initializer.databaseConfigurationProvider;
        jdbiTenantMap = new ConcurrentHashMap<>();
    }

    private DatabaseConfiguration getDatabaseConfigurationForTenant(String tenantId) {
        return databaseConfigurationProvider.get(tenantId).orElseThrow(() ->
                new DatabaseConfigurationException("Cannot find database configuration for tenant " + tenantId));
    }

    public Function<DatabaseConfiguration, DataSource> getDataSourceProvider() {
        return dataSourceProvider;
    }

    public DatabaseConfigurationProvider getDatabaseConfigurationProvider() {
        return databaseConfigurationProvider;
    }

    public Jdbi getJdbi() {
        return jdbiTenantMap.computeIfAbsent(currentTenantResolver.get(),
                tenantId -> Jdbi.create(dataSourceProvider.apply(getDatabaseConfigurationForTenant(tenantId)))
                        .installPlugin(new MultiTenantJdbiPlugin(currentTenantResolver,
                                databaseConfigurationProvider)));
    }

    public static final class Initializer {

        private Supplier<String> currentTenantResolver;
        private Function<DatabaseConfiguration, DataSource> dataSourceProvider;
        private DatabaseConfigurationProvider databaseConfigurationProvider;

        private Initializer() {
        }

        public Initializer setCurrentTenantResolver(Supplier<String> currentTenantResolver) {
            this.currentTenantResolver = currentTenantResolver;
            return this;
        }

        public Initializer setDataSourceProvider(Function<DatabaseConfiguration, DataSource> dataSourceProvider) {
            this.dataSourceProvider = dataSourceProvider;
            return this;
        }

        public Initializer setDatabaseConfigurationProvider(DatabaseConfigurationProvider databaseConfigurationProvider) {
            this.databaseConfigurationProvider = databaseConfigurationProvider;
            return this;
        }

        public JdbiFactory init() {
            if (INSTANCE == null) {
                checkNotNull(currentTenantResolver, "Current tenant resolver is required.");
                checkNotNull(dataSourceProvider, "Datasource provider is required.");
                checkNotNull(databaseConfigurationProvider, "Database configuration provider is required.");
                INSTANCE = new JdbiFactory(this);
            }
            return INSTANCE;
        }
    }
}
