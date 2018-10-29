package com.microsoft.sqlserver.jdbc.resiliency;

import static org.junit.Assert.assertTrue;

import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import org.junit.jupiter.api.Test;

import com.microsoft.sqlserver.testframework.AbstractTest;

public class ReflectiveTests extends AbstractTest {
    
    @Test
    public void testTimeout() throws SQLException {
        try (Connection c = DriverManager.getConnection(connectionString)) {
            try (Statement s = c.createStatement()) {
                ResiliencyUtils.killConnection(c, connectionString);
                ResiliencyUtils.blockConnection(c);
                s.executeQuery("SELECT 1");
            } catch (SQLException e) {
                assertTrue("Unexpected exception caught: " + e.getMessage(), e.getMessage().contains("timeout"));
            }
        }
    }
    
    @Test
    public void testDefaultRetry() throws SQLException {
        try (Connection c = DriverManager.getConnection(connectionString)) {
            try (Statement s = c.createStatement()) {
                ResiliencyUtils.killConnection(c, connectionString);
                ResiliencyUtils.blockConnection(c);
                s.executeQuery("SELECT 1");
            } catch (SQLException e) {
                assertTrue("Unexpected exception caught: " + e.getMessage(), e.getMessage().contains("timeout"));
            }
        }
    }
}
