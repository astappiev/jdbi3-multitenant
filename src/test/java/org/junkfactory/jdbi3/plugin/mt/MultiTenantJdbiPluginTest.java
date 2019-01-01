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

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junkfactory.jdbi3.plugin.mt.configuration.DatabaseConfiguration;
import org.junkfactory.jdbi3.plugin.mt.provider.DatabaseConfigurationProvider;
import org.junkfactory.jdbi3.plugin.mt.resolver.TenantResolver;
import org.junkfactory.jdbi3.plugin.mt.resolver.ThreadLocalTenantResolver;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Optional;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class MultiTenantJdbiPluginTest {

    @Mock
    Connection mockConnection;
    @Mock
    DatabaseConfigurationProvider mockDatabaseConfigurationProvider;
    @Mock
    DatabaseConfiguration mockDatabaseConfiguration;
    @Captor
    ArgumentCaptor<String> sqlQueryCaptor;

    @Test
    public void testThatPluginDoesNotExecuteSwitchDatabaseStatementOneSingleTenant() throws SQLException {

        final String tenant1 = "tenant1";

        TenantResolver mockTenantResolver = mock(TenantResolver.class);
        doReturn(tenant1).when(mockTenantResolver).get();
        doReturn(1).when(mockDatabaseConfigurationProvider).getNumTenants();
        doReturn(Optional.of(mockDatabaseConfiguration)).when(mockDatabaseConfigurationProvider).get(eq(tenant1));

        MultiTenantJdbiPlugin multiTenantJdbiPlugin = new MultiTenantJdbiPlugin(mockTenantResolver, mockDatabaseConfigurationProvider);
        multiTenantJdbiPlugin.customizeConnection(mockConnection);

        //get number of tenants is called once
        verify(mockDatabaseConfigurationProvider, times(1)).getNumTenants();
        //only 1 tenant, verify connection.createStatement() was not called
        verify(mockConnection, times(0)).createStatement();

    }

    @Test
    public void testThatPluginExecutesSwitchStatement() throws SQLException {

        final String defaultTenant = "default_tenant";
        final String defaultTenantDatabaseName = defaultTenant + "_prod";
        final String tenant1 = "tenant1";
        final String tenant1DatabaseName = tenant1 + "_prod";

        Statement mockStatement = mock(Statement.class);
        DatabaseConfiguration tenant1DatabaseConfiguration = mock(DatabaseConfiguration.class);
        DatabaseConfiguration defaultTenantDatabaseConfiguration = mock(DatabaseConfiguration.class);

        doReturn(defaultTenantDatabaseName).when(defaultTenantDatabaseConfiguration).getDatabaseName();
        doReturn(tenant1DatabaseName).when(tenant1DatabaseConfiguration).getDatabaseName();
        doReturn(mockStatement).when(mockConnection).createStatement();
        doReturn(2).when(mockDatabaseConfigurationProvider).getNumTenants();
        doReturn(Optional.of(tenant1DatabaseConfiguration)).when(mockDatabaseConfigurationProvider).get(eq(tenant1));
        doReturn(Optional.of(defaultTenantDatabaseConfiguration)).when(mockDatabaseConfigurationProvider).get(eq(defaultTenant));

        ThreadLocalTenantResolver tenantResolver = mock(ThreadLocalTenantResolver.class);
        doReturn(defaultTenant).when(tenantResolver).getDefaultTenant();

        MultiTenantJdbiPlugin multiTenantJdbiPlugin = new MultiTenantJdbiPlugin(tenantResolver, mockDatabaseConfigurationProvider);

        //test tenant1
        doReturn(tenant1).when(tenantResolver).get();
        multiTenantJdbiPlugin.customizeConnection(mockConnection);

        //get number of tenants is called once
        verify(mockDatabaseConfigurationProvider, times(1)).getNumTenants();
        //only 1 tenant, verify connection.createStatement() was not called
        verify(mockConnection, times(1)).createStatement();
        //verify use query
        verify(mockStatement, times(1)).execute(sqlQueryCaptor.capture());
        assertEquals("SQL Query must match", "USE " + tenant1DatabaseName, sqlQueryCaptor.getValue());


        doReturn(defaultTenant).when(tenantResolver).get();
        multiTenantJdbiPlugin.customizeConnection(mockConnection);

        //get number of tenants is called once
        verify(mockDatabaseConfigurationProvider, times(2)).getNumTenants();
        //only 1 tenant, verify connection.createStatement() was not called
        verify(mockConnection, times(2)).createStatement();
        //verify use query
        verify(mockStatement, times(2)).execute(sqlQueryCaptor.capture());
        assertEquals("SQL Query must match", "USE " + defaultTenantDatabaseName, sqlQueryCaptor.getValue());

    }

}