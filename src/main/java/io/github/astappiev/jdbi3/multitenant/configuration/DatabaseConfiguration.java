package io.github.astappiev.jdbi3.multitenant.configuration;

import java.io.Serializable;
import java.util.Objects;

import static org.jdbi.v3.core.generic.internal.Preconditions.checkArgument;

public class DatabaseConfiguration implements Serializable {

    private static final long serialVersionUID = -8293126037354414243L;

    private final String databaseName;
    private final String host;
    private final int port;
    private final String username;
    private final String password;

    private DatabaseConfiguration(Builder builder) {
        databaseName = builder.databaseName.trim();
        host = builder.host.trim();
        port = builder.port;
        username = builder.username.trim();
        password = builder.password.trim();
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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DatabaseConfiguration that = (DatabaseConfiguration) o;
        return port == that.port &&
                Objects.equals(databaseName, that.databaseName) &&
                Objects.equals(host, that.host) &&
                Objects.equals(username, that.username) &&
                Objects.equals(password, that.password);
    }

    @Override
    public int hashCode() {
        return Objects.hash(databaseName, host, port, username, password);
    }

    @Override
    public String toString() {
        return "DatabaseConfiguration{" +
                "databaseName='" + databaseName + '\'' +
                ", host='" + host + '\'' +
                ", port=" + port +
                ", username='" + username + '\'' +
                ", password='" + (password == null || password.isEmpty() ? "not set" : "set") + '\'' +
                '}';
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
            checkArgument(databaseName != null && databaseName.trim().length() > 0, "Database name is required");
            checkArgument(host != null && host.trim().length() > 0, "Host is required");
            checkArgument(port > 0, "Port is invalid");
            checkArgument(username != null && username.trim().length() > 0, "Username is required");
            checkArgument(password != null, "Password is required");
            return new DatabaseConfiguration(this);
        }
    }
}
