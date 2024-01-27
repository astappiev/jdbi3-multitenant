package io.github.astappiev.jdbi3.multitenant.resolver;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class ThreadLocalTenantResolverTest {

    private static final Logger logger = LoggerFactory.getLogger(ThreadLocalTenantResolverTest.class);

    private static final String TEST_DEFAULT_TENANT = "TEST_DEFAULT_TENANT";

    @Test
    public void testResolverIsPerThread() throws InterruptedException {

        try {
            ThreadLocalTenantResolver.newInitializer().init();
            fail("Must throw IllegalArgumentException cause of not setting default tenant");
        } catch (IllegalArgumentException e) {
            assertEquals("Message must match", "Default tenant is required", e.getMessage());
        }

        ThreadLocalTenantResolver resolver = ThreadLocalTenantResolver.newInitializer()
                .setDefaultTenant(TEST_DEFAULT_TENANT)
                .init();
        assertEquals("Instance must match", resolver, ThreadLocalTenantResolver.getInstance());
        assertEquals("Must match default tenant", TEST_DEFAULT_TENANT, resolver.get());
        assertEquals("Must match default tenant", TEST_DEFAULT_TENANT, resolver.getDefaultTenant());

        final String thread1Tenant = "th1Tenant";
        final String thread2Tenant = "th2Tenant";
        final String thread3Tenant = "th3Tenant";

        ExecutorService executorService = Executors.newFixedThreadPool(3);
        executorService.execute(() -> {
            logger.info("Setting tenant={}", thread2Tenant);
            resolver.setCurrentTenant(thread2Tenant);
            assertEquals("Tenant must match", thread2Tenant, resolver.get());
        });
        executorService.execute(() -> {
            logger.info("Setting tenant={}", thread1Tenant);
            resolver.setCurrentTenant(thread1Tenant);
            assertEquals("Tenant must match", thread1Tenant, resolver.get());
        });
        executorService.execute(() -> {
            logger.info("Setting tenant={}", thread3Tenant);
            resolver.setCurrentTenant(thread3Tenant);
            assertEquals("Tenant must match", thread3Tenant, resolver.get());
        });

        assertEquals("Must match default tenant", TEST_DEFAULT_TENANT, resolver.get());
        assertEquals("Must match default tenant", TEST_DEFAULT_TENANT, resolver.getDefaultTenant());

        executorService.shutdown();
        executorService.awaitTermination(10, TimeUnit.SECONDS);

    }

}