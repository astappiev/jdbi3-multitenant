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

import static org.jdbi.v3.core.generic.internal.Preconditions.checkArgument;

public class ThreadContextTenantResolver implements TenantResolver {

    private static ThreadContextTenantResolver instance;

    public static Initializer newInitializer() {
        return new Initializer();
    }

    public static ThreadContextTenantResolver getInstance() {
        return instance;
    }

    private final ThreadLocal<String> currentTenantHolder;
    private final String defaultTenant;

    private ThreadContextTenantResolver(Initializer initializer) {
        defaultTenant = initializer.defaultTenant;
        currentTenantHolder = ThreadLocal.withInitial(() -> defaultTenant);
    }

    @Override
    public String get() {
        return currentTenantHolder.get();
    }

    @Override
    public String getDefaultTenant() {
        return defaultTenant;
    }

    public void setCurrentTenant(String currentTenant) {
        currentTenantHolder.set(currentTenant);
    }

    public void reset() {
        currentTenantHolder.set(defaultTenant);
    }

    public static final class Initializer {
        private String defaultTenant;

        private Initializer() {
        }

        public Initializer setDefaultTenant(String defaultTenant) {
            this.defaultTenant = defaultTenant;
            return this;
        }

        public ThreadContextTenantResolver init() {
            if (instance == null) {
                checkArgument(defaultTenant != null && defaultTenant.trim().length() > 0,
                        "Default tenant is required");
                instance = new ThreadContextTenantResolver(this);
            }
            return instance;
        }
    }
}
