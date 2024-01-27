package io.github.astappiev.jdbi3.multitenant.configuration;

public class DatabaseConfigurationException extends RuntimeException {
    private static final long serialVersionUID = 4267596520744147966L;

    public DatabaseConfigurationException(String s) {
        super(s);
    }

    public DatabaseConfigurationException(String s, Throwable throwable) {
        super(s, throwable);
    }
}
