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

import org.jdbi.v3.core.Handle;
import org.jdbi.v3.core.Jdbi;
import org.junkfactory.jdbi3.plugin.mt.configuration.DatabaseConfiguration;
import org.junkfactory.jdbi3.plugin.mt.configuration.DatabaseConfigurationException;
import org.junkfactory.jdbi3.plugin.mt.provider.DatabaseConfigurationProvider;
import org.junkfactory.jdbi3.plugin.mt.resolver.TenantResolver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.jdbi.v3.core.generic.internal.Preconditions.checkNotNull;

public class JdbiTenantRegistry {

    private static final Logger logger = LoggerFactory.getLogger(JdbiTenantRegistry.class);
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
    private final Predicate<Handle> optionalConnectionTester;

    private final ConcurrentMap<String, Jdbi> jdbiTenantMap;
    private final MultiTenantJdbiPlugin multiTenantJdbiPlugin;

    private JdbiTenantRegistry(Initializer initializer) {
        currentTenantResolver = initializer.currentTenantResolver;
        dataSourceProvider = initializer.dataSourceProvider;
        databaseConfigurationProvider = initializer.databaseConfigurationProvider;
        optionalConnectionTester = initializer.connectionTester;
        jdbiTenantMap = new ConcurrentHashMap<>();
        multiTenantJdbiPlugin = new MultiTenantJdbiPlugin(currentTenantResolver, databaseConfigurationProvider);
    }

    private DatabaseConfiguration getDatabaseConfigurationForTenant(String tenantId) {
        Optional<DatabaseConfiguration> optionalDatabaseConfiguration = databaseConfigurationProvider.get(tenantId);
        return optionalDatabaseConfiguration.orElseThrow(() ->
                new DatabaseConfigurationException("Cannot find database configuration for tenant " + tenantId));
    }

    /**
     * Create a new {@link Jdbi} instance for tenantId
     * @param tenantId The tenant id
     * @return A new {@link Jdbi} instance
     */
    Jdbi createJdbi(String tenantId) {
        logger.debug("Creating new jdbi for {}", tenantId);
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

    /**
     * Get the current number of cached {@link Jdbi} instances
     * @return The current number of cached instances
     */
    public int getNumJdbiInstances() {
        return jdbiTenantMap.size();
    }

    /**
     * Get the cached {@link Jdbi} instance for the tenant resolved by {@link JdbiTenantRegistry#currentTenantResolver}.<br/>
     * If a {@link Jdbi} instance does not currently exist for the tenant, it creates a new instance
     * @return A {@link Jdbi} instance
     */
    public Jdbi getJdbi() {
        return getJdbi(currentTenantResolver.get());
    }

    /**
     * Get the cached {@link Jdbi} for tenantId. Creates a new instance when there's no instance yet.
     * @param tenantId The tenant id
     * @return A cached {@link Jdbi} instance for tenantId
     */
    public Jdbi getJdbi(String tenantId) {
        return jdbiTenantMap.computeIfAbsent(tenantId, this::createJdbi);
    }

    /**
     * Performs a check on all cached {@link Jdbi} instances
     * @return A {@link Map} where the key is all the known tenant ids with a boolean value. True for a healthy tenant {@link Jdbi}, otherwise false.
     */
    public Map<String, Boolean> checkHandles() {
        return jdbiTenantMap.keySet().stream()
                .collect(Collectors.toMap(Function.identity(), this::checkHandle));
    }

    /**
     * Test the {@link Jdbi}'s {@link Handle} for a tenant identified by tenantId
     * @param tenantId The tenant id
     * @return true if the test was successful.
     */
    public boolean checkHandle(String tenantId) {
        try {
            return getJdbi(tenantId).withHandle(h -> Optional.ofNullable(optionalConnectionTester).orElse(h1 -> {
                try {
                    return h.getConnection().isValid(3);
                } catch (SQLException e) {
                    logger.error("Failed to test handle for tenant={}", tenantId, e);
                    return false;
                }
            }).test(h));
        } catch (Throwable e) {
            logger.error("Unexpected exception on checkHandle for tenant={}", tenantId, e);
            return false;
        }
    }

    public static final class Initializer {

        private TenantResolver currentTenantResolver;
        private Function<DatabaseConfiguration, DataSource> dataSourceProvider;
        private DatabaseConfigurationProvider databaseConfigurationProvider;
        private Predicate<Handle> connectionTester;

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

        public Initializer setConnectionTester(Predicate<Handle> connectionTester) {
            this.connectionTester = connectionTester;
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
