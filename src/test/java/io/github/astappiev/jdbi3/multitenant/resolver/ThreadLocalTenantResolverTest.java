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
    void testResolverWithoutTenant() {
        try {
            ThreadLocalTenantResolver.newInitializer().init();
            fail("Must throw IllegalArgumentException cause of not setting default tenant");
        } catch (NullPointerException e) {
            assertEquals("Default tenant is required", e.getMessage());
        }
    }

    @Test
    void testResolverIsPerThread() throws InterruptedException {
        ThreadLocalTenantResolver resolver = ThreadLocalTenantResolver.newInitializer().setDefaultTenant(TEST_DEFAULT_TENANT).init();
        assertEquals(resolver, ThreadLocalTenantResolver.getInstance());
        assertEquals(TEST_DEFAULT_TENANT, resolver.get());
        assertEquals(TEST_DEFAULT_TENANT, resolver.getDefaultTenant());

        final String thread1Tenant = "th1Tenant";
        final String thread2Tenant = "th2Tenant";
        final String thread3Tenant = "th3Tenant";

        ExecutorService executorService = Executors.newFixedThreadPool(3);
        executorService.execute(() -> {
            logger.info("Setting tenant={}", thread2Tenant);
            resolver.setCurrentTenant(thread2Tenant);
            assertEquals(thread2Tenant, resolver.get());
        });
        executorService.execute(() -> {
            logger.info("Setting tenant={}", thread1Tenant);
            resolver.setCurrentTenant(thread1Tenant);
            assertEquals(thread1Tenant, resolver.get());
        });
        executorService.execute(() -> {
            logger.info("Setting tenant={}", thread3Tenant);
            resolver.setCurrentTenant(thread3Tenant);
            assertEquals(thread3Tenant, resolver.get());
        });

        assertEquals(TEST_DEFAULT_TENANT, resolver.get());
        assertEquals(TEST_DEFAULT_TENANT, resolver.getDefaultTenant());

        executorService.shutdown();
        executorService.awaitTermination(10, TimeUnit.SECONDS);
    }
}
