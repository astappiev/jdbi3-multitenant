package io.github.astappiev.jdbi3.multitenant.configuration;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class DatabaseConfigurationTest {

    private static final String DRIVER = "org.mariadb.jdbc.Driver";
    private static final String JDBC_URL = "jdbc:mariadb://localhost:3306/test";
    private static final String USERNAME = "username";
    private static final String PASSWORD = "password";

    private void verifyValidationMessage(DatabaseConfiguration.Builder builder, String expectedMessage) {
        try {
            builder.build();
            fail("Must throw NullPointerException");
        } catch (NullPointerException e) {
            assertEquals(expectedMessage, e.getMessage());
        }
    }

    @Test
    void testValidations() {
        DatabaseConfiguration.Builder builder = DatabaseConfiguration.newBuilder();
        builder.setDriverClassName(DRIVER);
        verifyValidationMessage(builder, "JDBC URL is required");
        verifyValidationMessage(builder.setJdbcUrl(JDBC_URL), "Username is required");
        verifyValidationMessage(builder.setUsername(USERNAME), "Password is required");
        DatabaseConfiguration databaseConfiguration = builder.setPassword(PASSWORD).build();

        assertEquals(DRIVER, databaseConfiguration.getDriverClassName());
        assertEquals(JDBC_URL, databaseConfiguration.getJdbcUrl());
        assertEquals(USERNAME, databaseConfiguration.getUsername());
        assertEquals(PASSWORD, databaseConfiguration.getPassword());
        assertEquals("test", databaseConfiguration.getDatabaseName());
    }

    @Test
    void testDatabaseNameExtraction() {
        DatabaseConfiguration conf = DatabaseConfiguration.newBuilder()
                .setJdbcUrl("jdbc:mariadb://localhost:3306/test?charset=utf8")
                .setUsername(USERNAME)
                .setPassword(PASSWORD)
                .build();

        assertEquals("jdbc:mariadb://localhost:3306/test?charset=utf8", conf.getJdbcUrl());
        assertEquals("test", conf.getDatabaseName());

        DatabaseConfiguration confSimpler = DatabaseConfiguration.newBuilder()
                .setJdbcUrl(JDBC_URL)
                .setUsername(USERNAME)
                .setPassword(PASSWORD)
                .build();

        assertEquals(JDBC_URL, confSimpler.getJdbcUrl());
        assertEquals("test", confSimpler.getDatabaseName());
    }

    @Test
    void testDatabaseNameReplacement() {
        DatabaseConfiguration conf = DatabaseConfiguration.newBuilder()
                .setJdbcUrl("jdbc:mariadb://localhost:3306/other?charset=utf8")
                .setDatabaseName("test")
                .setUsername(USERNAME)
                .setPassword(PASSWORD)
                .build();

        assertEquals("jdbc:mariadb://localhost:3306/test?charset=utf8", conf.getJdbcUrl());
        assertEquals("test", conf.getDatabaseName());

        DatabaseConfiguration confSimpler = DatabaseConfiguration.newBuilder()
                .setJdbcUrl("jdbc:mariadb://localhost:3306/other")
                .setDatabaseName("test")
                .setUsername(USERNAME)
                .setPassword(PASSWORD)
                .build();

        assertEquals(JDBC_URL, confSimpler.getJdbcUrl());
        assertEquals("test", confSimpler.getDatabaseName());
    }
}
