package io.github.astappiev.jdbi3.multitenant;

import com.zaxxer.hikari.HikariDataSource;
import io.github.astappiev.jdbi3.multitenant.configuration.DatabaseConfiguration;
import io.github.astappiev.jdbi3.multitenant.provider.CachedPerHostDataSourceProvider;
import io.github.astappiev.jdbi3.multitenant.provider.DatabaseConfigurationProvider;
import io.github.astappiev.jdbi3.multitenant.resolver.ThreadLocalTenantResolver;
import org.jdbi.v3.core.Jdbi;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
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

        ThreadLocalTenantResolver.newInitializer().setDefaultTenant(DEFAULT_TENANT).init();

        JdbiTenantRegistry.newInitializer()
                .setCurrentTenantResolver(ThreadLocalTenantResolver.getInstance())
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
                        return Optional.ofNullable(config);
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

        Map<String, Boolean> results = JdbiTenantRegistry.getInstance().checkHandles();
        logger.info("Checkhandle results={}", results);
        assertTrue("Result must match", results.get(DEFAULT_TENANT));
        assertTrue("Result must match", results.get(TENANT_1));
        assertTrue("Result must match", results.get(TENANT_2));

        assertFalse("Result must match", JdbiTenantRegistry.getInstance().checkHandle("unknown"));

    }

    private void testTenant(String tenantId) {

        final String tenantName = tenantId + "_name";
        ThreadLocalTenantResolver.getInstance().setCurrentTenant(tenantId);
        Jdbi jdbi = JdbiTenantRegistry.getInstance().getJdbi();
        Integer result = jdbi.withHandle(handle -> handle.select("select 1").mapTo(Integer.class).findOnly());
        assertEquals("Result must match", 1, result.intValue());

        logger.info("Creating {} for {}", tenantName, tenantId);
        Integer userId = jdbi.withHandle(handle -> handle.createUpdate("insert into user (name) values (:name)")
                .bind("name", tenantName)
                .executeAndReturnGeneratedKeys()
                .mapTo(Integer.class)
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

        ThreadLocalTenantResolver.getInstance().reset();

    }
}