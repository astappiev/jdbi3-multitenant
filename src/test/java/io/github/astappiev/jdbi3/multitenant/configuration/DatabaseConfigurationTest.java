package io.github.astappiev.jdbi3.multitenant.configuration;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class DatabaseConfigurationTest {

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
        final String driverClassName = "org.mariadb.jdbc.Driver";
        final String jdbcUrl = "jdbc:mariadb://localhost:3306/test";
        final String username = "username";
        final String password = "password";

        DatabaseConfiguration.Builder builder = DatabaseConfiguration.newBuilder();
        builder.setDriverClassName(driverClassName);
        verifyValidationMessage(builder, "JDBC URL is required");
        verifyValidationMessage(builder.setJdbcUrl(jdbcUrl), "Username is required");
        verifyValidationMessage(builder.setUsername(username), "Password is required");
        DatabaseConfiguration databaseConfiguration = builder.setPassword(password).build();

        assertEquals(driverClassName, databaseConfiguration.getDriverClassName());
        assertEquals(jdbcUrl, databaseConfiguration.getJdbcUrl());
        assertEquals(username, databaseConfiguration.getUsername());
        assertEquals(password, databaseConfiguration.getPassword());
        assertEquals("test", databaseConfiguration.getDatabaseName());
    }
}
