package io.github.astappiev.jdbi3.multitenant.resolver;

import java.util.function.Supplier;

/**
 * Contract for all tenant resolvers
 */
public interface TenantResolver extends Supplier<String> {

    /**
     * Get the current tenant
     *
     * @return The current tenant
     */
    @Override
    String get();

    /**
     * Get the default tenant
     *
     * @return The default tenant
     */
    String getDefaultTenant();

}
