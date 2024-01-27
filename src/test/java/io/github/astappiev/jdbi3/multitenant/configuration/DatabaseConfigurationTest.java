package io.github.astappiev.jdbi3.multitenant.configuration;

import org.junit.Test;

import static org.junit.Assert.*;

public class DatabaseConfigurationTest {

    private void verifyValidationMessage(DatabaseConfiguration.Builder builder, String expectedMessage) {
        try {
            builder.build();
            fail("Must throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            assertEquals("Message must match", expectedMessage, e.getMessage());
        }
    }

    @Test
    public void testValidations() {

        final String dbName = "dbname";
        final String host = "localhost";
        final int port = 3306;
        final String username = "username";
        final String password = "password";

        DatabaseConfiguration.Builder builder = DatabaseConfiguration.newBuilder();
        verifyValidationMessage(builder, "Database name is required");
        verifyValidationMessage(builder.setDatabaseName(dbName), "Host is required");
        verifyValidationMessage(builder.setHost(host), "Port is invalid");
        verifyValidationMessage(builder.setPort(port), "Username is required");
        verifyValidationMessage(builder.setUsername(username), "Password is required");
        DatabaseConfiguration databaseConfiguration = builder.setPassword(password).build();

        assertEquals("Must match", dbName, databaseConfiguration.getDatabaseName());
        assertEquals("Must match", host, databaseConfiguration.getHost());
        assertEquals("Must match", port, databaseConfiguration.getPort());
        assertEquals("Must match", username, databaseConfiguration.getUsername());
        assertEquals("Must match", password, databaseConfiguration.getPassword());

    }

}