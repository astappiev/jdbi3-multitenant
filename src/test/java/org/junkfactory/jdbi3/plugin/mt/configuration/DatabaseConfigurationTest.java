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

package org.junkfactory.jdbi3.plugin.mt.configuration;

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