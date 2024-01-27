package io.github.astappiev.jdbi3.multitenant.provider;

import io.github.astappiev.jdbi3.multitenant.configuration.DatabaseConfiguration;

import javax.sql.DataSource;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;

/**
 * Data data source provider cached per database host specified in {@link DatabaseConfiguration#getHost()}<br/>
 * Keeps a concurrent map holding references to {@link DataSource} where the key is the database host.<br/>
 * The data sources creation are delegated to {@link CachedPerHostDataSourceProvider#dataSourceProvider}
 */
public class CachedPerHostDataSourceProvider implements Function<DatabaseConfiguration, DataSource> {

    private final ConcurrentMap<String, DataSource> dataSourceMap;
    private final Function<DatabaseConfiguration, DataSource> dataSourceProvider;

    public CachedPerHostDataSourceProvider(Function<DatabaseConfiguration, DataSource> dataSourceProvider) {
        this.dataSourceProvider = dataSourceProvider;
        this.dataSourceMap = new ConcurrentHashMap<>();
    }

    @Override
    public DataSource apply(DatabaseConfiguration databaseConfiguration) {
        return dataSourceMap.computeIfAbsent(databaseConfiguration.getHost(), host -> dataSourceProvider.apply(databaseConfiguration));
    }
}
