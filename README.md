# MultiTenant Jdbi3 Plugin [![Build Status](https://travis-ci.org/junkfactory/mt-jdbi3-plugin.svg?branch=master)](https://travis-ci.org/junkfactory/mt-jdbi3-plugin)

A Jdbi3 plugin that adds support for schema per tenant setup for MySQL.

## Compile and Install

Use maven to compile and install the library. 

```
git clone https://github.com/junkfactory/mt-jdbi3-plugin.git
cd mt-jdbi3-plugin
mvn clean install

```

## Sample Usage

Initialize tenant resolver and JdbiTenantRegistry

```
ThreadContextTenantResolver.newInitializer().setDefaultTenant(DEFAULT_TENANT).init();

JdbiTenantRegistry.newInitializer()
        .setCurrentTenantResolver(ThreadContextTenantResolver.getInstance())
        .setDataSourceProvider(new CachedPerHostDataSourceProvider(config -> {

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

        }))
        .setDatabaseConfigurationProvider(new DatabaseConfigurationProvider() {

            @Override
            public int getNumTenants() {
                return 3;
            }

            @Override
            public Optional<DatabaseConfiguration> get(String tenantId) {
                DatabaseConfiguration config;
                switch (tenantId) {
                    case DEFAULT_TENANT:
                        config = DatabaseConfiguration.newBuilder()
                                .setHost("localhost")
                                .setPort(3306)
                                .setDatabaseName(DEFAULT_TENANT + "_dev")
                                .setUsername("root")
                                .setPassword("")
                                .build();
                        break;
                    case TENANT_1:
                        config = DatabaseConfiguration.newBuilder()
                                .setHost("localhost")
                                .setPort(3306)
                                .setDatabaseName(TENANT_1 + "_dev")
                                .setUsername("root")
                                .setPassword("")
                                .build();
                        break;
                    case TENANT_2:
                        config = DatabaseConfiguration.newBuilder()
                                .setHost("127.0.0.1")
                                .setPort(3306)
                                .setDatabaseName(TENANT_2 + "_dev")
                                .setUsername("root")
                                .setPassword("")
                                .build();
                        break;
                        default:
                            config = null;
                }
                return Optional.of(config);
            }
        }).init();
```
Set the current tenant typically from the start of the RR-loop
```
//set current tenant before getting a Jdbi instance from the registry
ThreadContextTenantResolver.getInstance().setCurrentTenant(tenantId);
Jdbi jdbi = JdbiTenantRegistry.getInstance().getJdbi();
Integer result = jdbi.withHandle(handle -> handle.select("select 1").mapTo(Integer.class).findOnly());
assertEquals("Result must match", 1, result.intValue());

Integer userId = jdbi.withHandle(handle -> handle.createUpdate("insert into user (name) values (:name)")
        .bind("name", "defaultTenant")
        .executeAndReturnGeneratedKeys()
        .map((rs, ctx) -> rs.getInt(1))
        .findOnly());
        
assertNotNull("User id must not be null", userId);
String name = jdbi.withHandle(handle -> handle.select("select name from user where id=?", userId).mapTo(String.class).findOnly());
assertEquals("Name must match", "defaultTenant", name);
```

## Limitations

* Only MySQL is currently supported
* Only tested with JDK 8

## License

This project is licensed under [MIT](https://opensource.org/licenses/MIT)
