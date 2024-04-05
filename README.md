# MultiTenant Jdbi3 Plugin

A plugin for [Jdbi3](https://github.com/jdbi/jdbi) that allows you to work with multiple databases in one application.
E.g. for a scenario when you have a separate database for each tenant/customer/application.
The plugin provides a tenant registry and a way to switch between them.

## How-to use

Include from Maven Central:

```xml
<dependency>
    <groupId>io.github.astappiev</groupId>
    <artifactId>jdbi3-multitenant</artifactId>
    <version>0.1.0</version>
</dependency>
```

## Sample Usage

Initialize tenant resolver and JdbiTenantRegistry

```java
ThreadLocalTenantResolver.newInitializer().setDefaultTenant(DEFAULT_TENANT).init();

JdbiTenantRegistry.newInitializer()
        .setCurrentTenantResolver(ThreadLocalTenantResolver.getInstance())
        .setDataSourceProvider(config -> {
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
        })
        .setDatabaseConfigurationProvider(tenantId -> {
            DatabaseConfiguration config;
            switch (tenantId) {
                case DEFAULT_TENANT:
                    config = DatabaseConfiguration.newBuilder()
                            .setJdbcUrl("jdbc:mariadb://localhost:3306/default_db")
                            .setUsername("root")
                            .setPassword("")
                            .build();
                    break;
                case TENANT_1:
                    config = DatabaseConfiguration.newBuilder()
                            .setJdbcUrl("jdbc:mariadb://localhost:3306/other_db")
                            .setUsername("tenant1")
                            .setPassword("")
                            .build();
                    break;
                case TENANT_2:
                    config = DatabaseConfiguration.newBuilder()
                            .setJdbcUrl("jdbc:mariadb://localhost:3306/second_db")
                            .setUsername("tenant2")
                            .setPassword("")
                            .build();
                    break;
                    default:
                        config = null;
            }
            return Optional.of(config);
        }).init();
```

Set the current tenant typically from the start of the RR-loop

```java
// Set the current tenant, before any Jdbi operation
ThreadLocalTenantResolver.getInstance().setCurrentTenant(tenantId);

// Use Jdbi from the registry
Jdbi jdbi = JdbiTenantRegistry.getInstance().getJdbi();
Integer result = jdbi.withHandle(handle -> handle.select("select 1").mapTo(Integer.class).findOnly());
assertEquals("Result must match", 1, result.intValue());

Integer userId = jdbi.withHandle(handle -> handle.createUpdate("insert into user (name) values (:name)")
        .bind("name", "defaultTenant")
        .executeAndReturnGeneratedKeys()
        .mapTo(Integer.class)
        .findOnly());
        
assertNotNull("User id must not be null", userId);
String name = jdbi.withHandle(handle -> handle.select("select name from user where id=?", userId).mapTo(String.class).findOnly());
assertEquals("Name must match", "defaultTenant", name);
```

## Limitations

* The plugin could keep many connections open. Set a minimum idle connection to 0 to avoid this.
* The fork of this plugin only supports separate connections for each tenant. The original implementation can be used with shared connection and catalog switching.

## License

This project is licensed under [MIT](https://opensource.org/licenses/MIT)
