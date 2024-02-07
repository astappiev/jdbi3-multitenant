package io.github.astappiev.jdbi3.multitenant;

import io.github.astappiev.jdbi3.multitenant.configuration.DatabaseConfiguration;
import io.github.astappiev.jdbi3.multitenant.configuration.DatabaseConfigurationException;
import io.github.astappiev.jdbi3.multitenant.provider.DatabaseConfigurationProvider;
import io.github.astappiev.jdbi3.multitenant.resolver.TenantResolver;
import org.jdbi.v3.core.Handle;
import org.jdbi.v3.core.spi.JdbiPlugin;
import org.jdbi.v3.core.statement.StatementBuilder;
import org.jdbi.v3.core.statement.StatementContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;

class MultiTenantJdbiPlugin implements JdbiPlugin {

    private static final Logger logger = LoggerFactory.getLogger(MultiTenantJdbiPlugin.class);

    private final TenantResolver currentTenantResolver;
    private final DatabaseConfigurationProvider databaseConfigurationProvider;

    public MultiTenantJdbiPlugin(TenantResolver currentTenantResolver, DatabaseConfigurationProvider databaseConfigurationProvider) {
        this.currentTenantResolver = currentTenantResolver;
        this.databaseConfigurationProvider = databaseConfigurationProvider;
    }

    @Override
    public Handle customizeHandle(Handle handle) {
        logger.debug("Customize handle statement builder");
        StatementBuilder realStatementBuilder = handle.getStatementBuilder();
        if (!(realStatementBuilder instanceof StatementBuilderDelegate)) {
            logger.debug("Installing statement builder delegate for {}", realStatementBuilder);
            handle.setStatementBuilder(new StatementBuilderDelegate(realStatementBuilder));
        }
        return handle;
    }

    @Override
    public Connection customizeConnection(Connection conn) {
        switchTenantDatabaseIfNecessary(conn);
        return conn;
    }

    protected void switchTenantDatabaseIfNecessary(Connection conn) {
        switchTenantDatabaseIfNecessary(conn, currentTenantResolver.get());
    }

    /**
     * Switch the tenant database. Only switches the database if there is more than 1 tenant
     *
     * @param conn The JDBC {@link Connection}
     */
    protected void switchTenantDatabaseIfNecessary(Connection conn, String currentTenant) {
        final DatabaseConfiguration databaseConfiguration = databaseConfigurationProvider.get(currentTenant)
            .orElseThrow(() -> new DatabaseConfigurationException("Cannot find database configuration for tenant " + currentTenant));
        final int numTenants = databaseConfigurationProvider.getNumTenants();
        logger.debug("Customize connection, tenant={}, database={}, numTenants={}", currentTenant, databaseConfiguration, numTenants);
        // customize connection only if number of tenants
        if (numTenants > 1) {
            try {
                conn.setCatalog(databaseConfiguration.getDatabaseName());
            } catch (SQLException e) {
                logger.error("Failed to switch db for tenant={}, database={}", currentTenant, databaseConfiguration);
                throw new IllegalStateException("Failed to switch db for tenant " + currentTenant, e);
            }
        }
    }

    protected void resetConnection(Connection conn) {
        if (!currentTenantResolver.getDefaultTenant().equals(currentTenantResolver.get())) {
            logger.debug("Resetting connection to default tenant={}", currentTenantResolver.getDefaultTenant());
            // reset to default tenant if it was switched to another
            switchTenantDatabaseIfNecessary(conn, currentTenantResolver.getDefaultTenant());
        }
    }

    static class StatementBuilderDelegate implements StatementBuilder {
        private final StatementBuilder delegate;

        public StatementBuilderDelegate(StatementBuilder delegate) {
            this.delegate = delegate;
        }

        @Override
        public Statement create(Connection conn, StatementContext ctx) throws SQLException {
            return delegate.create(conn, ctx);
        }

        @Override
        public PreparedStatement create(Connection conn, String sql, StatementContext ctx) throws SQLException {
            return delegate.create(conn, sql, ctx);
        }

        @Override
        public CallableStatement createCall(Connection conn, String sql, StatementContext ctx) throws SQLException {
            return delegate.createCall(conn, sql, ctx);
        }

        @Override
        public void close(Connection conn, String sql, Statement stmt) throws SQLException {
            delegate.close(conn, sql, stmt);
            // resetConnection(conn); FIXME: I don't understand why we need this
        }

        @Override
        public void close(Connection conn) {
            delegate.close(conn);
            // resetConnection(conn); FIXME: I don't understand why we need this
        }
    }

}
