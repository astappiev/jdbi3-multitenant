package io.github.astappiev.jdbi3.multitenant.resolver;

import static org.jdbi.v3.core.generic.internal.Preconditions.checkArgument;

public class ThreadLocalTenantResolver implements TenantResolver {

    private static ThreadLocalTenantResolver instance;

    public static Initializer newInitializer() {
        return new Initializer();
    }

    public static ThreadLocalTenantResolver getInstance() {
        return instance;
    }

    private final ThreadLocal<String> currentTenantHolder;
    private final String defaultTenant;

    private ThreadLocalTenantResolver(Initializer initializer) {
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

        public ThreadLocalTenantResolver init() {
            if (instance == null) {
                checkArgument(defaultTenant != null && defaultTenant.trim().length() > 0,
                        "Default tenant is required");
                instance = new ThreadLocalTenantResolver(this);
            } else {
                throw new IllegalStateException("ThreadLocalTenantResolver already initialized");
            }
            return instance;
        }
    }
}
