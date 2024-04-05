package io.github.astappiev.jdbi3.multitenant;

import io.github.astappiev.jdbi3.multitenant.configuration.DatabaseConfiguration;
import io.github.astappiev.jdbi3.multitenant.configuration.DatabaseConfigurationException;
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
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@Isolated
@ExtendWith(MockitoExtension.class)
class JdbiTenantRegistryTest {

    @Mock
    TenantResolver mockTenantResolver;

    @Mock
    Function<DatabaseConfiguration, DataSource> mockDataSourceProvider;

    @Mock
    Function<String, DatabaseConfiguration> mockDatabaseConfigurationProvider;

    @Mock
    Connection mockConnection;

    @Captor
    ArgumentCaptor<DatabaseConfiguration> databaseConfigurationArgumentCaptor;

    static String DEFAULT_TENANT = "default";
    static String TENANT_1 = "tenant1";
    static String TENANT_2 = "tenant2";

    static DatabaseConfiguration defaultDbConfig = DatabaseConfiguration.newBuilder()
            .setJdbcUrl("jdbc:mariadb://localhost:3306/" + DEFAULT_TENANT).setUsername("tenant_user").setPassword("").build();

    static DatabaseConfiguration tenant1DbConfig = DatabaseConfiguration.newBuilder()
            .setJdbcUrl("jdbc:mariadb://localhost:3306/" + TENANT_1).setUsername("tenant1_user").setPassword("").build();

    static DatabaseConfiguration tenant2DbConfig = DatabaseConfiguration.newBuilder()
            .setJdbcUrl("jdbc:mariadb://localhost:3306/" + TENANT_2).setUsername("tenant2_user").setPassword("").build();

    @BeforeEach
    void setUpEach() throws SQLException {
        JdbiTenantRegistry.releaseInstance();
        ThreadLocalTenantResolver.releaseInstance();

        lenient().doReturn(defaultDbConfig).when(mockDatabaseConfigurationProvider).apply(DEFAULT_TENANT);
        lenient().doReturn(tenant1DbConfig).when(mockDatabaseConfigurationProvider).apply(TENANT_1);
        lenient().doReturn(tenant2DbConfig).when(mockDatabaseConfigurationProvider).apply(TENANT_2);

        lenient().doReturn(DEFAULT_TENANT).when(mockTenantResolver).get();
        lenient().doReturn(DEFAULT_TENANT).when(mockTenantResolver).getDefaultTenant();

        DataSource mockDataSource = mock(DataSource.class);
        lenient().doReturn(mockDataSource).when(mockDataSourceProvider).apply(any(DatabaseConfiguration.class));
        lenient().doReturn(mockConnection).when(mockDataSource).getConnection();
        Statement mockStatement = mock(Statement.class);
        lenient().doReturn(mockStatement).when(mockConnection).createStatement();
    }

    @Test
    void testValidations() {
        JdbiTenantRegistry.Initializer initializer = JdbiTenantRegistry.newInitializer();
        assertThrowsExactly(NullPointerException.class, initializer::init, "Current tenant resolver is required.");
        assertThrowsExactly(NullPointerException.class, () ->
                initializer.setCurrentTenantResolver(mockTenantResolver).init(), "Database configuration provider is required.");
        assertThrowsExactly(NullPointerException.class, () ->
                initializer.setDatabaseConfigurationProvider(mockDatabaseConfigurationProvider).init(), "Data source provider is required.");
    }

    @Test
    void testUnknownTenant() {
        JdbiTenantRegistry.newInitializer()
            .setCurrentTenantResolver(mockTenantResolver)
            .setDatabaseConfigurationProvider(mockDatabaseConfigurationProvider)
            .setDataSourceProvider(mockDataSourceProvider)
            .init();

        doReturn("yet_another_tenant").when(mockTenantResolver).get();
        assertThrowsExactly(DatabaseConfigurationException.class, () -> JdbiTenantRegistry.getInstance().getJdbi());
    }

    @Test
    void testGetJdbi() {
        JdbiTenantRegistry.newInitializer()
            .setCurrentTenantResolver(mockTenantResolver)
            .setDatabaseConfigurationProvider(mockDatabaseConfigurationProvider)
            .setDataSourceProvider(mockDataSourceProvider)
            .init();

        assertSame(JdbiTenantRegistry.getInstance(), JdbiTenantRegistry.getInstance(), "Instance must match singleton instance");
        assertSame(mockTenantResolver, JdbiTenantRegistry.getInstance().getCurrentTenantResolver(), "Tenant resolver must match");
        assertSame(mockDatabaseConfigurationProvider, JdbiTenantRegistry.getInstance().getDatabaseConfigurationProvider(), "Database configuration provider must match");
        assertSame(mockDataSourceProvider, JdbiTenantRegistry.getInstance().getDataSourceProvider(), "Datasource provider must match");

        // number of jdbi instances remain the same as long as tenants are existing
        doReturn(TENANT_1).when(mockTenantResolver).get();
        JdbiTenantRegistry.getInstance().getJdbi();
        assertEquals(1, JdbiTenantRegistry.getInstance().getNumJdbiInstances());
        doReturn(TENANT_2).when(mockTenantResolver).get();
        JdbiTenantRegistry.getInstance().getJdbi();
        assertEquals(2, JdbiTenantRegistry.getInstance().getNumJdbiInstances());
        doReturn(TENANT_2).when(mockTenantResolver).get();
        JdbiTenantRegistry.getInstance().getJdbi();
        assertEquals(2, JdbiTenantRegistry.getInstance().getNumJdbiInstances());
        doReturn(TENANT_1).when(mockTenantResolver).get();
        JdbiTenantRegistry.getInstance().getJdbi();
        assertEquals(2, JdbiTenantRegistry.getInstance().getNumJdbiInstances());
        doReturn(DEFAULT_TENANT).when(mockTenantResolver).get();
        JdbiTenantRegistry.getInstance().getJdbi();
        assertEquals(3, JdbiTenantRegistry.getInstance().getNumJdbiInstances());
    }

    @Test
    void testJdbiHandle() throws SQLException {
        JdbiTenantRegistry.newInitializer()
            .setCurrentTenantResolver(mockTenantResolver)
            .setDatabaseConfigurationProvider(mockDatabaseConfigurationProvider)
            .setDataSourceProvider(mockDataSourceProvider)
            .init();

        testHandle(mockConnection, 1);

        doReturn(TENANT_1).when(mockTenantResolver).get();
        testHandle(mockConnection, 2);

        doReturn(TENANT_2).when(mockTenantResolver).get();
        testHandle(mockConnection, 3);

        // tenant 1 again
        doReturn(TENANT_1).when(mockTenantResolver).get();
        testHandle(mockConnection, 3);

        // tenant 2 again
        doReturn(TENANT_2).when(mockTenantResolver).get();
        testHandle(mockConnection, 3);
    }

    private void testHandle(Connection mockConnection, int instances) throws SQLException {
        Jdbi jdbi = JdbiTenantRegistry.getInstance().getJdbi();
        jdbi.useHandle(handle -> handle.select("select 1"));

        // num invocations remain the same
        verify(mockDataSourceProvider, times(instances)).apply(databaseConfigurationArgumentCaptor.capture());

        jdbi.useHandle(handle -> handle.select("select 1"));
        // num invocations remain the same
        verify(mockDataSourceProvider, times(instances)).apply(databaseConfigurationArgumentCaptor.capture());

        // instances remain the same
        assertEquals(instances, JdbiTenantRegistry.getInstance().getNumJdbiInstances());
        reset(mockConnection);
    }
}
