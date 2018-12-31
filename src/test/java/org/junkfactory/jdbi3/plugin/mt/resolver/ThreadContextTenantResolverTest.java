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

package org.junkfactory.jdbi3.plugin.mt.resolver;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class ThreadContextTenantResolverTest {

    private static final Logger logger = LoggerFactory.getLogger(ThreadContextTenantResolverTest.class);

    private static final String TEST_DEFAULT_TENANT = "TEST_DEFAULT_TENANT";

    @Test
    public void testValidationOfDefaultTenant() {

        try {
            ThreadContextTenantResolver resolver = ThreadContextTenantResolver.newInitializer().init();
            fail("Must throw IllegalArgumentException cause of not setting default tenant");
        } catch (IllegalArgumentException e) {
            assertEquals("Message must match", "Default tenant is required", e.getMessage());
        }

    }

    @Test
    public void testResolverIsPerThread() throws InterruptedException {

        ThreadContextTenantResolver resolver = ThreadContextTenantResolver.newInitializer()
                .setDefaultTenant(TEST_DEFAULT_TENANT)
                .init();
        assertEquals("Instance must match", resolver, ThreadContextTenantResolver.getInstance());
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