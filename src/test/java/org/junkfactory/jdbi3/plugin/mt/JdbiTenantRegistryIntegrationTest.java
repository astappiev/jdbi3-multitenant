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

import com.zaxxer.hikari.HikariDataSource;
import org.jdbi.v3.core.Jdbi;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junkfactory.jdbi3.plugin.mt.configuration.DatabaseConfiguration;
import org.junkfactory.jdbi3.plugin.mt.provider.CachedPerHostDataSourceProvider;
import org.junkfactory.jdbi3.plugin.mt.provider.DatabaseConfigurationProvider;
import org.junkfactory.jdbi3.plugin.mt.resolver.ThreadContextTenantResolver;
import org.mockito.junit.MockitoJUnitRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

@Category(IntegrationTest.class)
@RunWith(MockitoJUnitRunner.class)
public class JdbiTenantRegistryIntegrationTest {

    private static final Logger logger = LoggerFactory.getLogger(JdbiTenantRegistryIntegrationTest.class);

    private static final String DEFAULT_TENANT = "default";
    private static final String TENANT_1 = "tenant1";
    private static final String TENANT_2 = "tenant2";

    @BeforeClass
    public static void setUp() {

        ThreadContextTenantResolver.newInitializer().setDefaultTenant(DEFAULT_TENANT).init();

        JdbiTenantRegistry.newInitializer()
                .setCurrentTenantResolver(ThreadContextTenantResolver.getInstance())
                .setDataSourceProvider(new CachedPerHostDataSourceProvider(config -> {

                    String databaseOption = "serverTimezone=UTC&characterEncoding=UTF-8";
                    String jdbcUrl = String.format("jdbc:mysql://%s:%s/%s?%s",
                            config.getHost(),
                            config.getPort(),
                            config.getDatabaseName(),
                            databaseOption);

                    HikariDataSource dataSource = new HikariDataSource();
                    dataSource.setDriverClassName("com.mysql.jdbc.Driver");
                    dataSource.setJdbcUrl(jdbcUrl);
                    dataSource.setUsername(config.getUsername());
                    return dataSource;

                }))
                .setDatabaseConfigurationProvider(new DatabaseConfigurationProvider() {

                    @Override
                    public int getNumTenants() {
                        return 3;
                    }

                    @Override
                    public Optional<DatabaseConfiguration> get(String tenantId) {
                        DatabaseConfiguration config;
                        switch (tenantId) {
                            case DEFAULT_TENANT:
                                config = DatabaseConfiguration.newBuilder()
                                        .setHost("localhost")
                                        .setPort(3306)
                                        .setDatabaseName(DEFAULT_TENANT + "_dev")
                                        .setUsername("root")
                                        .setPassword("")
                                        .build();
                                break;
                            case TENANT_1:
                                config = DatabaseConfiguration.newBuilder()
                                        .setHost("localhost")
                                        .setPort(3306)
                                        .setDatabaseName(TENANT_1 + "_dev")
                                        .setUsername("root")
                                        .setPassword("")
                                        .build();
                                break;
                            case TENANT_2:
                                config = DatabaseConfiguration.newBuilder()
                                        .setHost("127.0.0.1")
                                        .setPort(3306)
                                        .setDatabaseName(TENANT_2 + "_dev")
                                        .setUsername("root")
                                        .setPassword("")
                                        .build();
                                break;
                                default:
                                    config = null;
                        }
                        return Optional.of(config);
                    }
                }).init();
    }

    @Test
    public void testJdbiRegistrySingleTenant() {

        JdbiTenantRegistry jdbiTenantRegistry = JdbiTenantRegistry.getInstance();
        Jdbi jdbi = jdbiTenantRegistry.getJdbi();
        Integer result = jdbi.withHandle(handle -> handle.select("select 1").mapTo(Integer.class).findOnly());
        assertEquals("Result must match", 1, result.intValue());

        Integer userId = jdbi.withHandle(handle -> handle.createUpdate("insert into user (name) values (:name)")
                .bind("name", "defaultTenant")
                .executeAndReturnGeneratedKeys()
                .map((rs, ctx) -> rs.getInt(1))
                .findOnly());
        assertNotNull("User id must not be null", userId);
        String name = jdbi.withHandle(handle -> handle.select("select name from user where id=?", userId).mapTo(String.class).findOnly());
        assertEquals("Name must match", "defaultTenant", name);

    }

    @Test
    public void testMultiTenant() throws InterruptedException {

        ExecutorService executorService = Executors.newFixedThreadPool(3);
        executorService.execute(() -> testTenant(DEFAULT_TENANT));
        executorService.execute(() -> testTenant(TENANT_1));
        executorService.execute(() -> testTenant(DEFAULT_TENANT));
        executorService.execute(() -> testTenant(TENANT_2));
        executorService.execute(() -> testTenant(TENANT_1));
        executorService.execute(() -> testTenant(TENANT_1));
        executorService.execute(() -> testTenant(TENANT_2));
        executorService.shutdown();
        executorService.awaitTermination(10, TimeUnit.SECONDS);

    }

    private void testTenant(String tenantId) {

        final String tenantName = tenantId + "_name";
        Jdbi jdbi = JdbiTenantRegistry.getInstance().getJdbi();
        ThreadContextTenantResolver.getInstance().setCurrentTenant(tenantId);
        Integer result = jdbi.withHandle(handle -> handle.select("select 1").mapTo(Integer.class).findOnly());
        assertEquals("Result must match", 1, result.intValue());

        logger.info("Creating {} for {}", tenantName, tenantId);
        Integer userId = jdbi.withHandle(handle -> handle.createUpdate("insert into user (name) values (:name)")
                .bind("name", tenantName)
                .executeAndReturnGeneratedKeys()
                .map((rs, ctx) -> rs.getInt(1))
                .findOnly());
        logger.info("Success with tenantId={}, id={}", tenantId, userId);

        assertNotNull("User id must not be null", userId);
        String name = jdbi.withHandle(handle -> handle.select("select name from user where id=?", userId).mapTo(String.class).findOnly());
        assertEquals("Name must match", tenantName, name);
        logger.info("Tenant name matched, tenantId={}, name={}", tenantId, name);

        jdbi.withHandle(handle -> handle.select("select * from user").mapToMap().list()).forEach(user -> {
            logger.info("Tenant names: tenantId={}, id={}, name={}", tenantId, user.get("id"), user.get("name"));
            assertTrue("Name must start with tenantId", ((String)user.get("name")).startsWith(tenantId));
        });

        ThreadContextTenantResolver.getInstance().reset();

    }
}