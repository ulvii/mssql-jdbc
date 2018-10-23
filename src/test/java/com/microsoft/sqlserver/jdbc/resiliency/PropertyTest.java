package com.microsoft.sqlserver.jdbc.resiliency;

import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.ThreadLocalRandom;

import org.junit.jupiter.api.Test;

import com.microsoft.sqlserver.testframework.AbstractTest;

public class PropertyTest extends AbstractTest {
    
    private void testInvalidPropertyOverBrokenConnection(String prop, String val) throws SQLException {
        StringBuilder sb = new StringBuilder();
        sb.append(connectionString).append(";").append(prop).append("=").append(val).append(";");
        try (Connection c = DriverManager.getConnection(sb.toString())) {
            try (Statement s = c.createStatement()) {
                ResiliencyUtils.killConnection(c, connectionString);
                s.executeQuery("SELECT 1");
                fail();
            } catch (SQLException e) {
                
            }
        }
    }
    
    @Test
    public void testRetryCount() throws SQLException {
        // fail immediately without retrying
        testInvalidPropertyOverBrokenConnection("connectRetryCount", "0");
        // Out of range, < 0
        testInvalidPropertyOverBrokenConnection("connectRetryCount",
                String.valueOf(ThreadLocalRandom.current().nextInt(Integer.MIN_VALUE, 0)));
        // Out of range, > 255
        testInvalidPropertyOverBrokenConnection("connectRetryCount",
                String.valueOf(ThreadLocalRandom.current().nextInt(256, Integer.MAX_VALUE)));
        // non-Integer types: boolean, float, double, string
        testInvalidPropertyOverBrokenConnection("connectRetryCount",
                String.valueOf(ThreadLocalRandom.current().nextBoolean()));
        testInvalidPropertyOverBrokenConnection("connectRetryCount",
                String.valueOf(ThreadLocalRandom.current().nextFloat()));
        testInvalidPropertyOverBrokenConnection("connectRetryCount",
                String.valueOf(ThreadLocalRandom.current().nextDouble()));
        testInvalidPropertyOverBrokenConnection("connectRetryCount",
                ResiliencyUtils.getRandomString(ResiliencyUtils.alpha, 15));
        //null
        testInvalidPropertyOverBrokenConnection("connectRetryCount","");
    }

    
    @Test
    public void testRetryInterval() throws SQLException {
        // Out of range, < 1
        testInvalidPropertyOverBrokenConnection("connectRetryInterval",
                String.valueOf(ThreadLocalRandom.current().nextInt(Integer.MIN_VALUE, 1)));
        // Out of range, > 60
        testInvalidPropertyOverBrokenConnection("connectRetryInterval",
                String.valueOf(ThreadLocalRandom.current().nextInt(61, Integer.MAX_VALUE)));
        // non-Integer types: boolean, float, double, string
        testInvalidPropertyOverBrokenConnection("connectRetryInterval",
                String.valueOf(ThreadLocalRandom.current().nextBoolean()));
        testInvalidPropertyOverBrokenConnection("connectRetryInterval",
                String.valueOf(ThreadLocalRandom.current().nextFloat()));
        testInvalidPropertyOverBrokenConnection("connectRetryInterval",
                String.valueOf(ThreadLocalRandom.current().nextDouble()));
        testInvalidPropertyOverBrokenConnection("connectRetryInterval",
                ResiliencyUtils.getRandomString(ResiliencyUtils.alpha, 15));
        //null
        testInvalidPropertyOverBrokenConnection("connectRetryInterval","");
    }
}
