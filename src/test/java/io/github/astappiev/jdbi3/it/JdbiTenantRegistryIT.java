package io.github.astappiev.jdbi3.it;

import com.zaxxer.hikari.HikariDataSource;
import io.github.astappiev.jdbi3.multitenant.JdbiTenantRegistry;
import io.github.astappiev.jdbi3.multitenant.configuration.DatabaseConfiguration;
import io.github.astappiev.jdbi3.multitenant.provider.CachedPerHostDataSourceProvider;
import io.github.astappiev.jdbi3.multitenant.provider.DatabaseConfigurationProvider;
import io.github.astappiev.jdbi3.multitenant.resolver.ThreadLocalTenantResolver;
import org.jdbi.v3.core.Jdbi;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.MariaDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.*;

import static org.junit.jupiter.api.Assertions.*;

@Testcontainers
@ExtendWith(MockitoExtension.class)
public class JdbiTenantRegistryIT {

    private static final Logger logger = LoggerFactory.getLogger(JdbiTenantRegistryIT.class);

    @Container
    private static final MariaDBContainer<?> MARIADB_DEFAULT = new MariaDBContainer<>("mariadb:10.11")
        .withDatabaseName("default");

    @Container
    private static final MariaDBContainer<?> MARIADB_TENANT1 = new MariaDBContainer<>("mariadb:10.11")
        .withDatabaseName("db1").withUsername("tenant1").withPassword("tenant1");

    @Container
    private static final MariaDBContainer<?> MARIADB_TENANT2 = new MariaDBContainer<>("mariadb:10.11")
        .withDatabaseName("db2").withUsername("tenant2").withPassword("tenant2");

    private static final String DEFAULT_TENANT = "default";
    private static final String TENANT_1 = "tenant1";
    private static final String TENANT_2 = "tenant2";

    @BeforeAll
    public static void setUp() {
        JdbiTenantRegistry.releaseInstance();
        ThreadLocalTenantResolver.releaseInstance();

        ThreadLocalTenantResolver.newInitializer().setDefaultTenant(DEFAULT_TENANT).init();

        JdbiTenantRegistry.newInitializer()
            .setCurrentTenantResolver(ThreadLocalTenantResolver.getInstance())
            .setDataSourceProvider(new CachedPerHostDataSourceProvider(config -> {
                HikariDataSource dataSource = new HikariDataSource();
                dataSource.setDriverClassName(Optional.ofNullable(config.getDriverClassName()).orElse("org.mariadb.jdbc.Driver"));
                dataSource.setJdbcUrl(config.getJdbcUrl());
                dataSource.setUsername(config.getUsername());
                dataSource.setPassword(config.getPassword());
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
                                    .setJdbcUrl(MARIADB_DEFAULT.getJdbcUrl())
                                    .setUsername(MARIADB_DEFAULT.getUsername())
                                    .setPassword(MARIADB_DEFAULT.getPassword())
                                    .build();
                            break;
                        case TENANT_1:
                            config = DatabaseConfiguration.newBuilder()
                                    .setJdbcUrl(MARIADB_TENANT1.getJdbcUrl())
                                    .setUsername(MARIADB_TENANT1.getUsername())
                                    .setPassword(MARIADB_TENANT1.getPassword())
                                    .build();
                            break;
                        case TENANT_2:
                            config = DatabaseConfiguration.newBuilder()
                                    .setJdbcUrl(MARIADB_TENANT2.getJdbcUrl())
                                    .setUsername(MARIADB_TENANT2.getUsername())
                                    .setPassword(MARIADB_TENANT2.getPassword())
                                    .build();
                            break;
                        default:
                            config = null;
                            break;
                    }
                    return Optional.ofNullable(config);
                }
            }).init();
    }

    @Test
    void testJdbiRegistrySingleTenant() {
        JdbiTenantRegistry jdbiTenantRegistry = JdbiTenantRegistry.getInstance();
        Jdbi jdbi = jdbiTenantRegistry.getJdbi();
        Integer result = jdbi.withHandle(handle -> handle.select("select 1").mapTo(Integer.class).one());
        assertEquals(1, result.intValue());

        jdbi.withHandle(handle -> handle.createScript("CREATE TABLE IF NOT EXISTS user (id INT NOT NULL AUTO_INCREMENT, name VARCHAR(100) NOT NULL, PRIMARY KEY (id))")
            .execute());
        Integer userId = jdbi.withHandle(handle -> handle.createUpdate("insert into user (name) values (:name)")
            .bind("name", "defaultTenant")
            .executeAndReturnGeneratedKeys()
            .map((rs, ctx) -> rs.getInt(1))
            .one());
        assertNotNull(userId, "User id must not be null");
        String name = jdbi.withHandle(handle -> handle.select("select name from user where id=?", userId).mapTo(String.class).one());
        assertEquals("defaultTenant", name);
    }

    @Test
    void testMultiTenant() throws InterruptedException, ExecutionException {
        ExecutorService executorService = Executors.newFixedThreadPool(3);
        List<Future<?>> futures = new ArrayList<>();
        futures.add(executorService.submit(() -> testTenant(DEFAULT_TENANT)));
        futures.add(executorService.submit(() -> testTenant(DEFAULT_TENANT)));
        futures.add(executorService.submit(() -> testTenant(TENANT_1)));
        futures.add(executorService.submit(() -> testTenant(DEFAULT_TENANT)));
        futures.add(executorService.submit(() -> testTenant(TENANT_2)));
        futures.add(executorService.submit(() -> testTenant(TENANT_1)));
        futures.add(executorService.submit(() -> testTenant(TENANT_1)));
        futures.add(executorService.submit(() -> testTenant(TENANT_2)));
        executorService.shutdown();
        executorService.awaitTermination(10, TimeUnit.SECONDS);
        futures.forEach(future -> {
            try {
                assertNull(future.get());
            } catch (InterruptedException | ExecutionException e) {
                fail(e);
            }
        });

        Map<String, Boolean> results = JdbiTenantRegistry.getInstance().checkHandles();
        logger.info("Checkhandle results={}", results);
        assertTrue(results.get(DEFAULT_TENANT));
        assertTrue(results.get(TENANT_1));
        assertTrue(results.get(TENANT_2));
    }

    @Test
    void testUnknownTenant() {
        assertFalse(JdbiTenantRegistry.getInstance().checkHandle("unknown"));
    }

    private void testTenant(String tenantId) {
        final String tenantName = tenantId + "_name";
        ThreadLocalTenantResolver.getInstance().setCurrentTenant(tenantId);

        Jdbi jdbi = JdbiTenantRegistry.getInstance().getJdbi();
        Integer result = jdbi.withHandle(handle -> handle.select("select 1").mapTo(Integer.class).one());
        assertEquals(1, result.intValue());
        jdbi.withHandle(handle -> handle.createScript("CREATE TABLE IF NOT EXISTS user (id INT NOT NULL AUTO_INCREMENT, name VARCHAR(100) NOT NULL, PRIMARY KEY (id))")
            .execute());

        logger.info("Creating {} for {}", tenantName, tenantId);
        Integer userId = jdbi.withHandle(handle -> handle.createUpdate("insert into user (name) values (:name)")
            .bind("name", tenantName)
            .executeAndReturnGeneratedKeys()
            .mapTo(Integer.class)
            .one());
        logger.info("Success with tenantId={}, id={}", tenantId, userId);

        assertNotNull(userId, "User id must not be null");
        String name = jdbi.withHandle(handle -> handle.select("select name from user where id=?", userId).mapTo(String.class).one());
        assertEquals(tenantName, name);
        logger.info("Tenant name matched, tenantId={}, name={}", tenantId, name);

        jdbi.withHandle(handle -> handle.select("select * from user").mapToMap().list()).forEach(user -> {
            logger.info("Tenant names: tenantId={}, id={}, name={}", tenantId, user.get("id"), user.get("name"));
            assertTrue(((String) user.get("name")).startsWith(tenantId), "Name must start with tenantId");
        });

        ThreadLocalTenantResolver.getInstance().reset();
    }
}
