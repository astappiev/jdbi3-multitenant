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
import org.junkfactory.jdbi3.plugin.mt.resolver.TenantResolver;

import javax.sql.DataSource;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;

import static org.jdbi.v3.core.generic.internal.Preconditions.checkNotNull;

public class JdbiTenantRegistry {

    private static JdbiTenantRegistry instance;

    public static JdbiTenantRegistry getInstance() {
        return instance;
    }

    public static Initializer newInitializer() {
        return new Initializer();
    }

    private final TenantResolver currentTenantResolver;
    private final Function<DatabaseConfiguration, DataSource> dataSourceProvider;
    private final DatabaseConfigurationProvider databaseConfigurationProvider;

    private final ConcurrentMap<String, Jdbi> jdbiTenantMap;
    private final MultiTenantJdbiPlugin multiTenantJdbiPlugin;

    private JdbiTenantRegistry(Initializer initializer) {
        currentTenantResolver = initializer.currentTenantResolver;
        dataSourceProvider = initializer.dataSourceProvider;
        databaseConfigurationProvider = initializer.databaseConfigurationProvider;
        jdbiTenantMap = new ConcurrentHashMap<>();
        multiTenantJdbiPlugin = new MultiTenantJdbiPlugin(currentTenantResolver, databaseConfigurationProvider);
    }

    private DatabaseConfiguration getDatabaseConfigurationForTenant(String tenantId) {
        Optional<DatabaseConfiguration> optionalDatabaseConfiguration = databaseConfigurationProvider.get(tenantId);
        return optionalDatabaseConfiguration.orElseThrow(() ->
                new DatabaseConfigurationException("Cannot find database configuration for tenant " + tenantId));
    }

    Jdbi createJdbi(String tenantId) {
        return Jdbi.create(dataSourceProvider.apply(getDatabaseConfigurationForTenant(tenantId)))
                .installPlugin(multiTenantJdbiPlugin);
    }

    public Function<DatabaseConfiguration, DataSource> getDataSourceProvider() {
        return dataSourceProvider;
    }

    public DatabaseConfigurationProvider getDatabaseConfigurationProvider() {
        return databaseConfigurationProvider;
    }

    public TenantResolver getCurrentTenantResolver() {
        return currentTenantResolver;
    }

    public int getNumJdbiInstances() {
        return jdbiTenantMap.size();
    }

    public Jdbi getJdbi() {
        return jdbiTenantMap.computeIfAbsent(currentTenantResolver.get(), this::createJdbi);
    }

    public static final class Initializer {

        private TenantResolver currentTenantResolver;
        private Function<DatabaseConfiguration, DataSource> dataSourceProvider;
        private DatabaseConfigurationProvider databaseConfigurationProvider;

        private Initializer() {
        }

        public Initializer setCurrentTenantResolver(TenantResolver currentTenantResolver) {
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

        public JdbiTenantRegistry init() {
            if (instance == null) {
                checkNotNull(currentTenantResolver, "Current tenant resolver is required.");
                checkNotNull(databaseConfigurationProvider, "Database configuration provider is required.");
                checkNotNull(dataSourceProvider, "Data source provider is required.");
                instance = new JdbiTenantRegistry(this);
            } else {
                throw new IllegalStateException("JdbiTenantRegistry already initialized");
            }
            return instance;
        }
    }
}
