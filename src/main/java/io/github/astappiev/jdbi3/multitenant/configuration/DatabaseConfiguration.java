package io.github.astappiev.jdbi3.multitenant.configuration;

import java.io.Serializable;
import java.util.Objects;

public class DatabaseConfiguration implements Serializable {
    private static final long serialVersionUID = -8293126037354414243L;

    private final String driverClassName;
    private final String jdbcUrl;
    private final String databaseName;
    private final String username;
    private final String password;

    private DatabaseConfiguration(Builder builder) {
        if (builder.driverClassName != null) {
            driverClassName = builder.driverClassName.trim();
        } else {
            driverClassName = null;
        }

        String jdbc = builder.jdbcUrl.trim();
        int hostIndex = jdbc.lastIndexOf("/") + 1;
        int queryIndex = jdbc.indexOf('?');

        if (builder.databaseName == null) {
            databaseName = jdbc.substring(hostIndex, queryIndex >= 0 ? queryIndex : jdbc.length());
            jdbcUrl = jdbc;
        } else {
            String jdbcPrefix = jdbc.substring(0, jdbc.lastIndexOf("/") + 1);
            String jdbcSuffix = queryIndex >= 0 ? jdbc.substring(queryIndex) : "";

            databaseName = builder.databaseName.trim();
            jdbcUrl = jdbcPrefix + databaseName + jdbcSuffix;
        }
        username = builder.username.trim();
        password = builder.password.trim();
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public String getDriverClassName() {
        return driverClassName;
    }

    public String getJdbcUrl() {
        return jdbcUrl;
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DatabaseConfiguration that = (DatabaseConfiguration) o;
        return Objects.equals(driverClassName, that.driverClassName) &&
            Objects.equals(jdbcUrl, that.jdbcUrl) &&
            Objects.equals(username, that.username) &&
            Objects.equals(password, that.password);
    }

    @Override
    public int hashCode() {
        return Objects.hash(driverClassName, jdbcUrl, username, password);
    }

    @Override
    public String toString() {
        return "DatabaseConfiguration{" +
            "databaseName='" + driverClassName + '\'' +
            ", host='" + jdbcUrl + '\'' +
            ", username='" + username + '\'' +
            ", password='" + (password == null || password.isEmpty() ? "not set" : "set") + '\'' +
            '}';
    }

    public static final class Builder {
        private String driverClassName;
        private String jdbcUrl;
        private String databaseName;
        private String username;
        private String password;

        private Builder() {
        }

        public Builder setDriverClassName(String driverClassName) {
            this.driverClassName = driverClassName;
            return this;
        }

        public Builder setJdbcUrl(String jdbcUrl) {
            this.jdbcUrl = jdbcUrl;
            return this;
        }

        /**
         * Set the database name. If set, the database name will override the database in JDBC URL.
         */
        public Builder setDatabaseName(String databaseName) {
            this.databaseName = databaseName;
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

        public Builder copy() {
            Builder builder = new Builder();
            builder.driverClassName = driverClassName;
            builder.jdbcUrl = jdbcUrl;
            builder.databaseName = databaseName;
            builder.username = username;
            builder.password = password;
            return builder;
        }

        public DatabaseConfiguration build() {
            Objects.requireNonNull(jdbcUrl, "JDBC URL is required");
            Objects.requireNonNull(username, "Username is required");
            Objects.requireNonNull(password, "Password is required");
            return new DatabaseConfiguration(this);
        }
    }
}
