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
    public void testTimeout() throws SQLException, IllegalArgumentException, IllegalAccessException, NoSuchFieldException, SecurityException {
        try (Connection c = DriverManager.getConnection(connectionString)) {
            try (Statement s = c.createStatement()) {
                ResiliencyUtils.killConnection(c, connectionString);
                assertTrue("Failed to block connection", blockConnection(c));
                s.executeQuery("SELECT 1");
            } catch (SQLException e) {
                assertTrue(e.getMessage().contains("timeout"));
            }
        }
    }
    
    
    
    
    //uses reflection to "corrupt" a Connection's server target
    private boolean blockConnection(Connection c) throws SQLException, IllegalArgumentException, IllegalAccessException, NoSuchFieldException, SecurityException {
        Class<?> connectionClass = c.getClass().getSuperclass();
        Field fields[] = connectionClass.getDeclaredFields();
        for (Field f : fields) {
            System.out.println(f.getName());
            if (f.getName() == "activeConnectionProperties" && Properties.class == f.getType()) {
                f.setAccessible(true);
                Properties connectionProps = (Properties) f.get(c);
                connectionProps.setProperty("serverName", "BOGUS-SERVER-NAME");
                f.set(c,connectionProps);
                return true;
            }
        }
        return false;
    }
}
