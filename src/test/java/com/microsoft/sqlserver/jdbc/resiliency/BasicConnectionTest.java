package com.microsoft.sqlserver.jdbc.resiliency;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.jupiter.api.Test;

import com.microsoft.sqlserver.jdbc.RandomUtil;
import com.microsoft.sqlserver.jdbc.TestUtils;
import com.microsoft.sqlserver.testframework.AbstractTest;


public class BasicConnectionTest extends AbstractTest {

    @Test
    public void testBasicReconnectDefault() throws SQLException {
        basicReconnect(connectionString);
    }

    @Test
    public void testBasicEncryptedConnection() throws SQLException {
        basicReconnect(connectionString + ";encrypt=true;trustServerCertificate=true;");
    }

    @Test
    public void testGracefulClose() throws SQLException {
        try (Connection c = DriverManager.getConnection(connectionString)) {
            try (Statement s = c.createStatement()) {
                ResiliencyUtils.killConnection(c, connectionString);
                c.close();
                s.executeQuery("SELECT 1");
                fail("Query execution did not throw an exception on a closed execution");
            } catch (SQLException e) {
                assertTrue(e.getMessage().contains("the connection is closed."));
            }
        }
    }

    @Test
    public void testCatalog() throws SQLException {
        String databaseName = null;
        try (Connection c = DriverManager.getConnection(connectionString); Statement s = c.createStatement()) {
            try {
                databaseName = RandomUtil.getIdentifier("resDB");
                TestUtils.dropDatabaseIfExists(databaseName, s);
                s.execute("CREATE DATABASE " + databaseName);
                try {
                    c.setCatalog(databaseName);
                } catch (SQLException e) {
                    // Switching databases is not supported against Azure, skip/
                    return;
                }
                ResiliencyUtils.killConnection(c, connectionString);
                try (ResultSet rs = s.executeQuery("SELECT db_name();")) {
                    while (rs.next()) {
                        // Check if the driver reconnected to the expected database. 
                        assertEquals(databaseName, rs.getString(1));
                    }
                }
            } finally {
                TestUtils.dropDatabaseIfExists(databaseName, s);
            }
        }
    }

    private void basicReconnect(String connectionString) throws SQLException {
        try (Connection c = DriverManager.getConnection(connectionString)) {
            try (Statement s = c.createStatement()) {
                ResiliencyUtils.killConnection(c, connectionString);
                s.executeQuery("SELECT 1");
            }
        }
    }
}
