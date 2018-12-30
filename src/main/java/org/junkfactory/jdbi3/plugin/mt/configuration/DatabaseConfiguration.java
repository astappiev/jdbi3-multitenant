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

import java.io.Serializable;

public class DatabaseConfiguration implements Serializable {

    private static final long serialVersionUID = 2594727374644897025L;

    private final String databaseName;
    private final String host;
    private final int port;
    private final String username;
    private final String password;

    private DatabaseConfiguration(Builder builder) {
        databaseName = builder.databaseName;
        host = builder.host;
        port = builder.port;
        username = builder.username;
        password = builder.password;
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }

    public static final class Builder {
        private String databaseName;
        private String host;
        private int port;
        private String username;
        private String password;

        private Builder() {
        }

        public Builder setDatabaseName(String databaseName) {
            this.databaseName = databaseName;
            return this;
        }

        public Builder setHost(String host) {
            this.host = host;
            return this;
        }

        public Builder setPort(int port) {
            this.port = port;
            return this;
        }

        public Builder setUsername(String username) {
            this.username = username;
            return this;
        }

        public Builder setPassword(String password) {
            this.password = password;
            return this;
        }

        public DatabaseConfiguration build() {
            return new DatabaseConfiguration(this);
        }
    }
}
