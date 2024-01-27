package io.github.astappiev.jdbi3.multitenant.provider;

import io.github.astappiev.jdbi3.multitenant.configuration.DatabaseConfiguration;

import java.util.Optional;

/**
 * Database configuration provider contract
 */
public interface DatabaseConfigurationProvider {

    int getNumTenants();
    Optional<DatabaseConfiguration> get(String tenantId);

}
