package io.github.astappiev.jdbi3.multitenant;

import io.github.astappiev.jdbi3.multitenant.configuration.DatabaseConfiguration;
import io.github.astappiev.jdbi3.multitenant.provider.DatabaseConfigurationProvider;
import io.github.astappiev.jdbi3.multitenant.resolver.TenantResolver;
import io.github.astappiev.jdbi3.multitenant.resolver.ThreadLocalTenantResolver;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class MultiTenantJdbiPluginTest {

    @Mock
    Connection mockConnection;
    @Mock
    DatabaseConfigurationProvider mockDatabaseConfigurationProvider;
    @Mock
    DatabaseConfiguration mockDatabaseConfiguration;
    @Captor
    ArgumentCaptor<String> sqlQueryCaptor;

    @Test
    void testThatPluginDoesNotExecuteSwitchDatabaseStatementOneSingleTenant() throws SQLException {
        final String tenant1 = "tenant1";

        TenantResolver mockTenantResolver = mock(TenantResolver.class);
        doReturn(tenant1).when(mockTenantResolver).get();
        doReturn(1).when(mockDatabaseConfigurationProvider).getNumTenants();
        doReturn(Optional.of(mockDatabaseConfiguration)).when(mockDatabaseConfigurationProvider).get(tenant1);

        MultiTenantJdbiPlugin multiTenantJdbiPlugin = new MultiTenantJdbiPlugin(mockTenantResolver, mockDatabaseConfigurationProvider);
        multiTenantJdbiPlugin.customizeConnection(mockConnection);

        // get number of tenants is called once
        verify(mockDatabaseConfigurationProvider, times(1)).getNumTenants();
        // only 1 tenant, verify connection.createStatement() was not called
        verify(mockConnection, times(0)).createStatement();
    }

    @Test
    void testThatPluginExecutesSwitchStatement() throws SQLException {
        final String defaultTenant = "default_tenant";
        final String defaultTenantDatabaseName = defaultTenant + "_prod";
        final String defaultTenantJdbcUrl = "jdbc:mariadb://localhost:3306/" + defaultTenantDatabaseName;

        final String tenant1 = "tenant1";
        final String tenant1DatabaseName = tenant1 + "_prod";
        final String tenant1JdbcUrl = "jdbc:mariadb://localhost:3306/" + tenant1DatabaseName;

        DatabaseConfiguration defaultTenantDatabaseConfiguration = DatabaseConfiguration.newBuilder().setJdbcUrl(defaultTenantJdbcUrl).setUsername(defaultTenant).setPassword("").build();
        DatabaseConfiguration tenant1DatabaseConfiguration = DatabaseConfiguration.newBuilder().setJdbcUrl(tenant1JdbcUrl).setUsername(tenant1).setPassword("").build();

        doReturn(2).when(mockDatabaseConfigurationProvider).getNumTenants();
        doReturn(Optional.of(tenant1DatabaseConfiguration)).when(mockDatabaseConfigurationProvider).get(tenant1);
        doReturn(Optional.of(defaultTenantDatabaseConfiguration)).when(mockDatabaseConfigurationProvider).get(defaultTenant);

        ThreadLocalTenantResolver tenantResolver = mock(ThreadLocalTenantResolver.class);
        MultiTenantJdbiPlugin multiTenantJdbiPlugin = new MultiTenantJdbiPlugin(tenantResolver, mockDatabaseConfigurationProvider);

        // test tenant1
        doReturn(tenant1).when(tenantResolver).get();
        multiTenantJdbiPlugin.customizeConnection(mockConnection);

        // get number of tenants is called once
        verify(mockDatabaseConfigurationProvider, times(1)).getNumTenants();
        // verify set catalog
        verify(mockConnection, times(1)).setCatalog(sqlQueryCaptor.capture());
        assertEquals(tenant1DatabaseName, sqlQueryCaptor.getValue(), "SQL Query must match");

        doReturn(defaultTenant).when(tenantResolver).get();
        multiTenantJdbiPlugin.customizeConnection(mockConnection);

        // get number of tenants is called once
        verify(mockDatabaseConfigurationProvider, times(2)).getNumTenants();
        // verify set catalog
        verify(mockConnection, times(2)).setCatalog(sqlQueryCaptor.capture());
        assertEquals(defaultTenantDatabaseName, sqlQueryCaptor.getValue(), "SQL Query must match");
    }
}
