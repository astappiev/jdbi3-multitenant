package io.github.astappiev.jdbi3.multitenant.resolver;

import io.github.astappiev.jdbi3.multitenant.JdbiTenantRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Isolated;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

@Isolated
class ThreadLocalTenantResolverTest {

    private static final Logger logger = LoggerFactory.getLogger(ThreadLocalTenantResolverTest.class);

    private static final String TEST_DEFAULT_TENANT = "TEST_DEFAULT_TENANT";

    @BeforeEach
    void setUpEach() {
        JdbiTenantRegistry.releaseInstance();
        ThreadLocalTenantResolver.releaseInstance();
    }

    @Test
    void testThrowOnNoDefault() {
        try {
            ThreadLocalTenantResolver.newInitializer().init();
            fail("Must throw NullPointerException cause of not setting default tenant");
        } catch (NullPointerException e) {
            assertEquals("Default tenant is required", e.getMessage());
        }
    }

    @Test
    void testThrowOnMultipleInitialisations() {
        try {
            ThreadLocalTenantResolver.newInitializer().setDefaultTenant(TEST_DEFAULT_TENANT).init();
            ThreadLocalTenantResolver.newInitializer().setDefaultTenant(TEST_DEFAULT_TENANT).init();
            fail("Must throw NullPointerException cause of not setting default tenant");
        } catch (IllegalStateException e) {
            assertEquals("ThreadLocalTenantResolver already initialized", e.getMessage());
        }
    }

    @Test
    void testResolverIsPerThread() throws InterruptedException {
        ThreadLocalTenantResolver resolver = ThreadLocalTenantResolver.newInitializer().setDefaultTenant(TEST_DEFAULT_TENANT).init();
        assertEquals(resolver, ThreadLocalTenantResolver.getInstance());
        assertEquals(TEST_DEFAULT_TENANT, resolver.get());
        assertEquals(TEST_DEFAULT_TENANT, resolver.getDefaultTenant());

        ExecutorService executorService = Executors.newFixedThreadPool(5);
        for (int i = 0; i < 10; i++) {
            String tenantName = "th" + i + "Tenant";
            executorService.execute(() -> {
                logger.info("Setting tenant={}", tenantName);
                resolver.setCurrentTenant(tenantName);
                logger.info("Retrieving tenant={}", resolver.get());
                assertEquals(tenantName, resolver.get());
            });
        }

        assertEquals(TEST_DEFAULT_TENANT, resolver.get());
        assertEquals(TEST_DEFAULT_TENANT, resolver.getDefaultTenant());

        executorService.shutdown();
        executorService.awaitTermination(10, TimeUnit.SECONDS);
    }
}
