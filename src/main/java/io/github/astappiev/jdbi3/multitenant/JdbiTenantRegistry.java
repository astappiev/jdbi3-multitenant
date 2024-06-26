package io.github.astappiev.jdbi3.multitenant;

import io.github.astappiev.jdbi3.multitenant.configuration.DatabaseConfiguration;
import io.github.astappiev.jdbi3.multitenant.configuration.DatabaseConfigurationException;
import io.github.astappiev.jdbi3.multitenant.resolver.TenantResolver;
import io.github.astappiev.jdbi3.multitenant.resolver.ThreadLocalTenantResolver;
import org.jdbi.v3.core.Handle;
import org.jdbi.v3.core.Jdbi;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class JdbiTenantRegistry {

    private static final Logger logger = LoggerFactory.getLogger(JdbiTenantRegistry.class);
    private static JdbiTenantRegistry instance;
    private final TenantResolver currentTenantResolver;
    private final Function<DatabaseConfiguration, DataSource> dataSourceProvider;
    private final Function<String, DatabaseConfiguration> databaseConfigurationProvider;
    private final Function<Jdbi, Void> jdbiCustomizer;
    private final Predicate<Handle> optionalConnectionTester;
    private final ConcurrentMap<String, Jdbi> jdbiTenantMap;

    private JdbiTenantRegistry(Initializer initializer) {
        currentTenantResolver = initializer.currentTenantResolver;
        dataSourceProvider = initializer.dataSourceProvider;
        databaseConfigurationProvider = initializer.databaseConfigurationProvider;
        optionalConnectionTester = initializer.connectionTester;
        jdbiCustomizer = initializer.jdbiCustomizer;
        jdbiTenantMap = new ConcurrentHashMap<>();
    }

    public static JdbiTenantRegistry getInstance() {
        return instance;
    }

    public static void releaseInstance() {
        instance = null;
    }

    public static Initializer newInitializer() {
        return new Initializer();
    }

    private DatabaseConfiguration getDatabaseConfigurationForTenant(String tenantId) {
        Optional<DatabaseConfiguration> optionalDatabaseConfiguration = Optional.ofNullable(databaseConfigurationProvider.apply(tenantId));
        return optionalDatabaseConfiguration.orElseThrow(() ->
            new DatabaseConfigurationException("Cannot find database configuration for tenant " + tenantId));
    }

    /**
     * Create a new {@link Jdbi} instance for tenantId
     *
     * @param tenantId The tenant id
     * @return A new {@link Jdbi} instance
     */
    private Jdbi createJdbi(String tenantId) {
        logger.debug("Creating new jdbi for {}", tenantId);
        Jdbi jdbi = Jdbi.create(dataSourceProvider.apply(getDatabaseConfigurationForTenant(tenantId)));
        if (jdbiCustomizer != null) {
            jdbiCustomizer.apply(jdbi);
        }
        return jdbi;
    }

    public Function<DatabaseConfiguration, DataSource> getDataSourceProvider() {
        return dataSourceProvider;
    }

    public Function<String, DatabaseConfiguration> getDatabaseConfigurationProvider() {
        return databaseConfigurationProvider;
    }

    public TenantResolver getCurrentTenantResolver() {
        return currentTenantResolver;
    }

    /**
     * Get the current number of cached {@link Jdbi} instances
     *
     * @return The current number of cached instances
     */
    public int getNumJdbiInstances() {
        return jdbiTenantMap.size();
    }

    /**
     * Get the cached {@link Jdbi} instance for the tenant resolved by {@link JdbiTenantRegistry#currentTenantResolver}.<br/>
     * If a {@link Jdbi} instance does not currently exist for the tenant, it creates a new instance
     *
     * @return A {@link Jdbi} instance
     */
    public Jdbi getJdbi() {
        return getJdbi(currentTenantResolver.get());
    }

    /**
     * Get the cached {@link Jdbi} instance for the tenant resolved by {@link JdbiTenantRegistry#currentTenantResolver}.<br/>
     * If a {@link Jdbi} instance does not currently exist for the tenant, it creates a new instance
     *
     * @return A {@link Jdbi} instance
     */
    public Jdbi getDefaultJdbi() {
        return getJdbi(currentTenantResolver.getDefaultTenant());
    }

    /**
     * Get the cached {@link Jdbi} for tenantId. Creates a new instance when there's no instance yet.
     *
     * @param tenantId The tenant id
     * @return A cached {@link Jdbi} instance for tenantId
     */
    public Jdbi getJdbi(String tenantId) {
        return jdbiTenantMap.computeIfAbsent(tenantId, this::createJdbi);
    }

    /**
     * Performs a check on all cached {@link Jdbi} instances
     *
     * @return A {@link Map} where the key is all the known tenant ids with a boolean value. True for a healthy tenant {@link Jdbi}, otherwise false.
     */
    public Map<String, Boolean> checkHandles() {
        return jdbiTenantMap.keySet().stream().collect(Collectors.toMap(Function.identity(), this::checkHandle));
    }

    /**
     * Test the {@link Jdbi}'s {@link Handle} for a tenant identified by tenantId
     *
     * @param tenantId The tenant id
     * @return true if the test was successful.
     */
    public boolean checkHandle(String tenantId) {
        String currentTenant = currentTenantResolver.get();
        try {
            ThreadLocalTenantResolver.getInstance().setCurrentTenant(tenantId);
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
        } finally {
            ThreadLocalTenantResolver.getInstance().setCurrentTenant(currentTenant);
        }
    }

    public static final class Initializer {
        private TenantResolver currentTenantResolver;
        private Function<DatabaseConfiguration, DataSource> dataSourceProvider;
        private Function<String, DatabaseConfiguration> databaseConfigurationProvider;
        private Function<Jdbi, Void> jdbiCustomizer;
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

        public Initializer setDatabaseConfigurationProvider(Function<String, DatabaseConfiguration> databaseConfigurationProvider) {
            this.databaseConfigurationProvider = databaseConfigurationProvider;
            return this;
        }

        public Initializer setJdbiCustomizer(Function<Jdbi, Void> jdbiCustomizer) {
            this.jdbiCustomizer = jdbiCustomizer;
            return this;
        }

        public Initializer setConnectionTester(Predicate<Handle> connectionTester) {
            this.connectionTester = connectionTester;
            return this;
        }

        public JdbiTenantRegistry init() {
            if (instance == null) {
                Objects.requireNonNull(currentTenantResolver); // Current tenant resolver is required.
                Objects.requireNonNull(databaseConfigurationProvider); // Database configuration provider is required.
                Objects.requireNonNull(dataSourceProvider); // Data source provider is required.
                instance = new JdbiTenantRegistry(this);
            } else {
                throw new IllegalStateException("JdbiTenantRegistry already initialized");
            }

            return instance;
        }
    }
}
