package io.github.astappiev.jdbi3.multitenant;

import io.github.astappiev.jdbi3.multitenant.configuration.DatabaseConfiguration;
import io.github.astappiev.jdbi3.multitenant.provider.CachedPerHostDataSourceProvider;
import io.github.astappiev.jdbi3.multitenant.provider.DatabaseConfigurationProvider;
import io.github.astappiev.jdbi3.multitenant.resolver.TenantResolver;
import io.github.astappiev.jdbi3.multitenant.resolver.ThreadLocalTenantResolver;
import org.jdbi.v3.core.Jdbi;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.parallel.Isolated;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Optional;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;
import static org.mockito.Mockito.*;

@Isolated
@ExtendWith(MockitoExtension.class)
class JdbiTenantRegistryTest {

    @Mock
    TenantResolver mockTenantResolver;

    @Mock
    Function<DatabaseConfiguration, DataSource> mockDataSourceProvider;

    @Mock
    DatabaseConfigurationProvider mockDatabaseConfigurationProvider;

    @Captor
    ArgumentCaptor<DatabaseConfiguration> databaseConfigurationArgumentCaptor;

    @BeforeEach
    void setUpEach() {
        JdbiTenantRegistry.releaseInstance();
        ThreadLocalTenantResolver.releaseInstance();
    }

    @Test
    void testValidations() {
        JdbiTenantRegistry.Initializer initializer = JdbiTenantRegistry.newInitializer();
        assertThrowsExactly(NullPointerException.class, initializer::init, "Current tenant resolver is required.");
        assertThrowsExactly(NullPointerException.class, () -> initializer.setCurrentTenantResolver(mockTenantResolver)
            .init(), "Database configuration provider is required.");
        assertThrowsExactly(NullPointerException.class, () -> initializer.setDatabaseConfigurationProvider(mockDatabaseConfigurationProvider)
            .init(), "Data source provider is required.");
    }

    @Test
    void getJdbi() throws SQLException {
        final String defaultTenant = "defaultTenant";
        final String tenant1 = "tenant1";
        final String tenant2 = "tenant2";

        final String defaultDatabase = defaultTenant + "_prod";
        final String tenant1Database = tenant1 + "_prod";
        final String tenant2Database = tenant2 + "_prod";

        final String defaultJdbcUrl = "jdbc:mariadb://localhost:3306/" + defaultDatabase;
        final String tenant1JdbcUrl = "jdbc:mariadb://localhost:3306/" + tenant1Database;
        final String tenant2JdbcUrl = "jdbc:mariadb://localhost:3306/" + tenant2Database;

        DatabaseConfiguration defaultTenantConfig = mock(DatabaseConfiguration.class);
        doReturn(defaultDatabase).when(defaultTenantConfig).getDatabaseName();
        doReturn(defaultJdbcUrl).when(defaultTenantConfig).getJdbcUrl();

        DatabaseConfiguration tenant1Config = mock(DatabaseConfiguration.class);
        doReturn(tenant1Database).when(tenant1Config).getDatabaseName();
        doReturn(tenant1JdbcUrl).when(tenant1Config).getJdbcUrl();

        DatabaseConfiguration tenant2Config = mock(DatabaseConfiguration.class);
        doReturn(tenant2Database).when(tenant2Config).getDatabaseName();
        doReturn(tenant2JdbcUrl).when(tenant2Config).getJdbcUrl();

        doReturn(Optional.of(defaultTenantConfig)).when(mockDatabaseConfigurationProvider).get(defaultTenant);
        doReturn(Optional.of(tenant1Config)).when(mockDatabaseConfigurationProvider).get(tenant1);
        doReturn(Optional.of(tenant2Config)).when(mockDatabaseConfigurationProvider).get(tenant2);

        doReturn(3).when(mockDatabaseConfigurationProvider).getNumTenants();

        DataSource mockDataSource = mock(DataSource.class);
        doReturn(mockDataSource).when(mockDataSourceProvider).apply(any(DatabaseConfiguration.class));

        Connection mockConnection = mock(Connection.class);
        doReturn(mockConnection).when(mockDataSource).getConnection();

        Statement mockStatement = mock(Statement.class);
        doReturn(mockStatement).when(mockConnection).createStatement();

        doReturn(defaultTenant).when(mockTenantResolver).get();
        // doReturn(defaultTenant).when(mockTenantResolver).getDefaultTenant();

        CachedPerHostDataSourceProvider cachedPerHostDataSourceProvider = new CachedPerHostDataSourceProvider(mockDataSourceProvider);
        JdbiTenantRegistry.newInitializer()
            .setCurrentTenantResolver(mockTenantResolver)
            .setDatabaseConfigurationProvider(mockDatabaseConfigurationProvider)
            .setDataSourceProvider(cachedPerHostDataSourceProvider)
            .init();

        assertEquals(JdbiTenantRegistry.getInstance(), JdbiTenantRegistry.getInstance(), "Instance must match singleton instance");
        assertEquals(mockTenantResolver, JdbiTenantRegistry.getInstance().getCurrentTenantResolver(), "Tenant resolver must match");
        assertEquals(mockDatabaseConfigurationProvider, JdbiTenantRegistry.getInstance()
            .getDatabaseConfigurationProvider(), "Database configuration provider must match");
        assertEquals(cachedPerHostDataSourceProvider, JdbiTenantRegistry.getInstance().getDataSourceProvider(), "Datasource provider must match");

        ArgumentCaptor<String> sqlStringCaptor = ArgumentCaptor.forClass(String.class);

        // test jdbi with default tenant
        Jdbi jdbi = JdbiTenantRegistry.getInstance().getJdbi();
        jdbi.useHandle(handle -> handle.select("select 1"));
        verify(mockConnection, times(1)).setCatalog(sqlStringCaptor.capture());
        verify(mockDataSourceProvider, times(1)).apply(databaseConfigurationArgumentCaptor.capture());
        reset(mockStatement);
        jdbi.useHandle(handle -> handle.select("select 1"));
        verify(mockConnection, times(2)).setCatalog(sqlStringCaptor.capture());
        verify(mockDataSourceProvider, times(1)).apply(databaseConfigurationArgumentCaptor.capture());
        assertEquals(defaultJdbcUrl, databaseConfigurationArgumentCaptor.getValue().getJdbcUrl());
        assertEquals(1, JdbiTenantRegistry.getInstance().getNumJdbiInstances());

        // test jdbi tenant 1
        reset(mockConnection);
        sqlStringCaptor = ArgumentCaptor.forClass(String.class);

        doReturn(tenant1).when(mockTenantResolver).get();
        jdbi = JdbiTenantRegistry.getInstance().getJdbi();
        jdbi.useHandle(handle -> handle.select("select 1"));
        verify(mockConnection, times(1)).setCatalog(sqlStringCaptor.capture()); // 2
        assertEquals(tenant1Database, sqlStringCaptor.getAllValues().get(0));
        // assertEquals(defaultDatabase, sqlStringCaptor.getAllValues().get(1));
        // same host as default so num invocation still the same
        verify(mockDataSourceProvider, times(2)).apply(databaseConfigurationArgumentCaptor.capture());
        jdbi.useHandle(handle -> handle.select("select 1"));
        // same host as default so num invocation still the same
        verify(mockDataSourceProvider, times(2)).apply(databaseConfigurationArgumentCaptor.capture());
        assertEquals(tenant1JdbcUrl, databaseConfigurationArgumentCaptor.getValue().getJdbcUrl());
        assertEquals(2, JdbiTenantRegistry.getInstance().getNumJdbiInstances());

        reset(mockConnection);
        sqlStringCaptor = ArgumentCaptor.forClass(String.class);

        doReturn(tenant2).when(mockTenantResolver).get();
        jdbi = JdbiTenantRegistry.getInstance().getJdbi();
        jdbi.useHandle(handle -> handle.select("select 1"));
        verify(mockConnection, times(1)).setCatalog(sqlStringCaptor.capture()); // 2
        assertEquals(tenant2Database, sqlStringCaptor.getAllValues().get(0));
        // assertEquals(defaultDatabase, sqlStringCaptor.getAllValues().get(1));
        // diff host new invocation
        verify(mockDataSourceProvider, times(3)).apply(databaseConfigurationArgumentCaptor.capture());
        jdbi.useHandle(handle -> handle.select("select 1"));
        // diff host new invocation
        verify(mockDataSourceProvider, times(3)).apply(databaseConfigurationArgumentCaptor.capture());
        assertEquals(tenant2JdbcUrl, databaseConfigurationArgumentCaptor.getValue().getJdbcUrl());
        assertEquals(3, JdbiTenantRegistry.getInstance().getNumJdbiInstances());

        // tenant 1 again
        doReturn(tenant1).when(mockTenantResolver).get();
        jdbi = JdbiTenantRegistry.getInstance().getJdbi();
        jdbi.useHandle(handle -> handle.select("select 1"));
        // num invocations remain the same
        verify(mockDataSourceProvider, times(3)).apply(databaseConfigurationArgumentCaptor.capture());
        jdbi.useHandle(handle -> handle.select("select 1"));
        // num invocations remain the same
        verify(mockDataSourceProvider, times(3)).apply(databaseConfigurationArgumentCaptor.capture());
        // instances remain the same
        assertEquals(3, JdbiTenantRegistry.getInstance().getNumJdbiInstances());

        // number of jdbi instances remain the same as long as tenants are existing
        doReturn(tenant1).when(mockTenantResolver).get();
        JdbiTenantRegistry.getInstance().getJdbi();
        doReturn(tenant2).when(mockTenantResolver).get();
        JdbiTenantRegistry.getInstance().getJdbi();
        doReturn(tenant1).when(mockTenantResolver).get();
        JdbiTenantRegistry.getInstance().getJdbi();
        doReturn(defaultTenant).when(mockTenantResolver).get();
        JdbiTenantRegistry.getInstance().getJdbi();
        assertEquals(3, JdbiTenantRegistry.getInstance().getNumJdbiInstances());
    }
}
