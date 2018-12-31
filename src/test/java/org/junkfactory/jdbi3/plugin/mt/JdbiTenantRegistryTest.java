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
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junkfactory.jdbi3.plugin.mt.configuration.DatabaseConfiguration;
import org.junkfactory.jdbi3.plugin.mt.provider.CachedPerHostDataSourceProvider;
import org.junkfactory.jdbi3.plugin.mt.provider.DatabaseConfigurationProvider;
import org.junkfactory.jdbi3.plugin.mt.resolver.TenantResolver;
import org.junkfactory.jdbi3.plugin.mt.resolver.ThreadContextTenantResolver;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Optional;
import java.util.function.Function;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class JdbiTenantRegistryTest {

    private static final Logger logger = LoggerFactory.getLogger(JdbiTenantRegistryTest.class);

    @Mock
    TenantResolver mockTenantResolver;

    @Mock
    Function<DatabaseConfiguration, DataSource> mockDataSourceProvider;

    @Mock
    DatabaseConfigurationProvider mockDatabaseConfigurationProvider;

    @Captor
    ArgumentCaptor<DatabaseConfiguration> databaseConfigurationArgumentCaptor;

    @Test
    public void testValidations() {

        JdbiTenantRegistry.Initializer initializer = JdbiTenantRegistry.newInitializer();
        try {
            initializer.init();
            fail("Must throw NullPointerException");
        } catch (NullPointerException e) {
            assertEquals("Validation message must match", "Current tenant resolver is required.", e.getMessage());
        }
        try {
            initializer.setCurrentTenantResolver(mockTenantResolver).init();
            fail("Must throw NullPointerException");
        } catch (NullPointerException e) {
            assertEquals("Validation message must match", "Database configuration provider is required.", e.getMessage());
        }
        try {
            initializer.setDatabaseConfigurationProvider(mockDatabaseConfigurationProvider).init();
            fail("Must throw NullPointerException");
        } catch (NullPointerException e) {
            assertEquals("Validation message must match", "Data source provider is required.", e.getMessage());
        }

        JdbiTenantRegistry registry = initializer.setDataSourceProvider(mockDataSourceProvider).init();
        assertEquals("Instance must match singleton instance", registry, JdbiTenantRegistry.getInstance());
        assertEquals("Tenant resolver must match", mockTenantResolver, registry.getCurrentTenantResolver());
        assertEquals("Database configuration provider must match", mockDatabaseConfigurationProvider, registry.getDatabaseConfigurationProvider());
        assertEquals("Datasource provider must match", mockDataSourceProvider, registry.getDataSourceProvider());

    }

    @Test
    public void getJdbi() throws SQLException {

        final String defaultTenant = "defaultTenant";
        final String tenant1 = "tenant1";
        final String tenant2 = "tenant2";

        final String defaultDatabase = defaultTenant + "_prod";
        final String tenant1Database = tenant1 + "_prod";
        final String tenant2Database = tenant2 + "_prod";

        final String defaultHost = defaultTenant + ".com";
        final String tenant1Host = defaultHost; //tenant 1 host is same as default
        final String tenant2Host = tenant2 + ".com";

        DatabaseConfiguration defaultTenantConfig = mock(DatabaseConfiguration.class);
        doReturn(defaultDatabase).when(defaultTenantConfig).getDatabaseName();
        doReturn(defaultHost).when(defaultTenantConfig).getHost();

        DatabaseConfiguration tenant1Config = mock(DatabaseConfiguration.class);
        doReturn(tenant1Database).when(tenant1Config).getDatabaseName();
        doReturn(tenant1Host).when(tenant1Config).getHost();

        DatabaseConfiguration tenant2Config = mock(DatabaseConfiguration.class);
        doReturn(tenant2Database).when(tenant2Config).getDatabaseName();
        doReturn(tenant2Host).when(tenant2Config).getHost();

        doReturn(Optional.of(defaultTenantConfig)).when(mockDatabaseConfigurationProvider).get(eq(defaultTenant));
        doReturn(Optional.of(tenant1Config)).when(mockDatabaseConfigurationProvider).get(eq(tenant1));
        doReturn(Optional.of(tenant2Config)).when(mockDatabaseConfigurationProvider).get(eq(tenant2));

        doReturn(3).when(mockDatabaseConfigurationProvider).getNumTenants();

        DataSource mockDataSource = mock(DataSource.class);
        doReturn(mockDataSource).when(mockDataSourceProvider).apply(any(DatabaseConfiguration.class));

        Connection mockConnection = mock(Connection.class);
        doReturn(mockConnection).when(mockDataSource).getConnection();

        Statement mockStatement = mock(Statement.class);
        doReturn(mockStatement).when(mockConnection).createStatement();

        JdbiTenantRegistry.newInitializer()
                .setCurrentTenantResolver(ThreadContextTenantResolver.newInitializer().setDefaultTenant(defaultTenant).init())
                .setDatabaseConfigurationProvider(mockDatabaseConfigurationProvider)
                .setDataSourceProvider(new CachedPerHostDataSourceProvider(mockDataSourceProvider))
                .init();

        //test jdbi with default tenant
        Jdbi jdbi = JdbiTenantRegistry.getInstance().getJdbi();
        jdbi.useHandle(handle -> handle.select("select 1"));
        verify(mockDataSourceProvider, times(1)).apply(databaseConfigurationArgumentCaptor.capture());
        jdbi.useHandle(handle -> handle.select("select 1"));
        verify(mockDataSourceProvider, times(1)).apply(databaseConfigurationArgumentCaptor.capture());
        assertEquals("Host must match", defaultHost, databaseConfigurationArgumentCaptor.getValue().getHost());
        assertEquals("Jdbi num instances must match", 1, JdbiTenantRegistry.getInstance().getNumJdbiInstances());

        //test jdbi tenant 1
        ThreadContextTenantResolver.getInstance().setCurrentTenant(tenant1);
        jdbi = JdbiTenantRegistry.getInstance().getJdbi();
        jdbi.useHandle(handle -> handle.select("select 1"));
        //same host as default so num invocation still the same
        verify(mockDataSourceProvider, times(1)).apply(databaseConfigurationArgumentCaptor.capture());
        jdbi.useHandle(handle -> handle.select("select 1"));
        //same host as default so num invocation still the same
        verify(mockDataSourceProvider, times(1)).apply(databaseConfigurationArgumentCaptor.capture());
        assertEquals("Host must match", tenant1Host, databaseConfigurationArgumentCaptor.getValue().getHost());
        assertEquals("Jdbi num instances must match", 2, JdbiTenantRegistry.getInstance().getNumJdbiInstances());

        ThreadContextTenantResolver.getInstance().setCurrentTenant(tenant2);
        jdbi = JdbiTenantRegistry.getInstance().getJdbi();
        jdbi.useHandle(handle -> handle.select("select 1"));
        //diff host new invocation
        verify(mockDataSourceProvider, times(2)).apply(databaseConfigurationArgumentCaptor.capture());
        jdbi.useHandle(handle -> handle.select("select 1"));
        //diff host new invocation
        verify(mockDataSourceProvider, times(2)).apply(databaseConfigurationArgumentCaptor.capture());
        assertEquals("Host must match", tenant2Host, databaseConfigurationArgumentCaptor.getValue().getHost());
        assertEquals("Jdbi num instances must match", 3, JdbiTenantRegistry.getInstance().getNumJdbiInstances());

        //tenant 1 again
        ThreadContextTenantResolver.getInstance().setCurrentTenant(tenant1);
        jdbi = JdbiTenantRegistry.getInstance().getJdbi();
        jdbi.useHandle(handle -> handle.select("select 1"));
        //num invocations remain the same
        verify(mockDataSourceProvider, times(2)).apply(databaseConfigurationArgumentCaptor.capture());
        jdbi.useHandle(handle -> handle.select("select 1"));
        //num invocations remain the same
        verify(mockDataSourceProvider, times(2)).apply(databaseConfigurationArgumentCaptor.capture());
        //instances remain the same
        assertEquals("Jdbi num instances must match", 3, JdbiTenantRegistry.getInstance().getNumJdbiInstances());


        //number of jdbi instances remain the same as long as tenants are existing
        ThreadContextTenantResolver.getInstance().setCurrentTenant(tenant1);
        JdbiTenantRegistry.getInstance().getJdbi();
        ThreadContextTenantResolver.getInstance().setCurrentTenant(tenant2);
        JdbiTenantRegistry.getInstance().getJdbi();
        ThreadContextTenantResolver.getInstance().setCurrentTenant(tenant1);
        JdbiTenantRegistry.getInstance().getJdbi();
        ThreadContextTenantResolver.getInstance().setCurrentTenant(defaultTenant);
        JdbiTenantRegistry.getInstance().getJdbi();
        assertEquals("Jdbi num instances must match", 3, JdbiTenantRegistry.getInstance().getNumJdbiInstances());
    }
}