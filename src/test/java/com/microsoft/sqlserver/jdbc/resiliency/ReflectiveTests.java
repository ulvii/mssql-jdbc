package com.microsoft.sqlserver.jdbc.resiliency;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.Test;

import com.microsoft.sqlserver.jdbc.TestUtils;
import com.microsoft.sqlserver.testframework.AbstractTest;


public class ReflectiveTests extends AbstractTest {

    /*
     * Command with default ReconnectRetryDelay is executed over a broken connection Expected: Client waits exactly 10
     * seconds between attempts Expected to timeout in 20 seconds, (retryDelay * (retryCount-1)) +
     * (loginTimeout*retryCount)
     */
    @Test
    public void testDefaultTimeout() throws SQLException {
        long startTime = 0;
        Map<String, String> m = new HashMap<>();
        m.put("connectRetryCount", "2");
        m.put("loginTimeout", "5");
        String cs = ResiliencyUtils.setConnectionProps(connectionString.concat(";"), m);
        try (Connection c = DriverManager.getConnection(cs)) {
            try (Statement s = c.createStatement()) {
                ResiliencyUtils.killConnection(c, connectionString);
                ResiliencyUtils.blockConnection(c);
                startTime = System.currentTimeMillis();
                s.executeQuery("SELECT 1");
                fail("Successfully executed query on a blocked connection.");
            } catch (SQLException e) {
                double elapsedTime = (System.currentTimeMillis() - startTime);
                // Default attempt interval is 10 seconds, and login timeout is 5
                assertTrue("Elapsed Time out of Range: " + elapsedTime, elapsedTime > 20000 && elapsedTime < 25000);
            }
        }
    }

    /*
     * Default retry count is 1, and login timeout is 15. Expected: Times out in 15 seconds
     */
    @Test
    public void testDefaultRetry() throws SQLException {
        long startTime = 0;
        try (Connection c = DriverManager.getConnection(connectionString)) {
            try (Statement s = c.createStatement()) {
                ResiliencyUtils.killConnection(c, connectionString);
                ResiliencyUtils.blockConnection(c);
                startTime = System.currentTimeMillis();
                s.executeQuery("SELECT 1");
            } catch (SQLException e) {
                double elapsedTime = System.currentTimeMillis() - startTime;
                assertTrue("Timeout did not match expected Timeout of 15: " + elapsedTime,
                        elapsedTime < 16000 && elapsedTime > 14000);
            }
        }
    }

    /*
     * Command with ReconnectRetryDelay > QueryTimeout is executed over a broken connection Expected: Client reports
     * query timeout immediately without waiting for ReconenctRetryDelay
     */
    @Test
    public void testReconnectDelayQueryTimeout() throws SQLException {
        long startTime = 0;
        Map<String, String> m = new HashMap<>();
        m.put("connectRetryCount", "3");
        m.put("connectRetryInterval", "30");
        m.put("queryTimeout", "5");
        String cs = ResiliencyUtils.setConnectionProps(connectionString.concat(";"), m);
        try (Connection c = DriverManager.getConnection(cs)) {
            try (Statement s = c.createStatement()) {
                ResiliencyUtils.killConnection(c, connectionString);
                ResiliencyUtils.blockConnection(c);
                startTime = System.currentTimeMillis();
                s.executeQuery("SELECT 1");
                fail("Successfully executed query on a blocked connection.");
            } catch (SQLException e) {
                double elapsedTime = System.currentTimeMillis() - startTime;
                assertTrue("Query did not timeout in time, elapsed time(ms): " + elapsedTime, elapsedTime < 10000);
            }
        }
    }

    /*
     * Command with infinite ConnectionTimeout and ReconnectRetryCount == 1 is executed over a broken connection
     * Expected: Client times out by QueryTimeout
     */
    @Test
    public void testQueryTimeout() throws SQLException {
        long startTime = 0;
        Map<String, String> m = new HashMap<>();
        m.put("queryTimeout", "5");
        m.put("loginTimeout", "65535");
        m.put("connectRetryCount", "1");
        String cs = ResiliencyUtils.setConnectionProps(connectionString.concat(";"), m);
        try (Connection c = DriverManager.getConnection(cs)) {
            try (Statement s = c.createStatement()) {
                ResiliencyUtils.killConnection(c, connectionString);
                ResiliencyUtils.blockConnection(c);
                startTime = System.currentTimeMillis();
                s.executeQuery("SELECT 1");
                fail("Successfully executed query on a blocked connection.");
            } catch (SQLException e) {
                double elapsedTime = System.currentTimeMillis() - startTime;
                // Timeout should occur after query timeout and not login timeout
                assertTrue("Query did not timeout in time, elapsed time(ms): " + elapsedTime, elapsedTime < 10000);
            }
        }
    }

    /*
     * Command with QueryTimeout > (ReconnectRetryCount * (ReconnectRetryDelay + ConnectionTimeout)) is executed over a
     * broken connection Expected: Client fails after ReconnectRetryCount attempts
     */
    @Test
    public void testValidRetryWindow() throws SQLException {
        long startTime = 0;
        Map<String, String> m = new HashMap<>();
        m.put("queryTimeout", "-1");
        m.put("loginTimeout", "5");
        m.put("connectRetryCount", "2");
        m.put("connectRetryInterval", "10");
        String cs = ResiliencyUtils.setConnectionProps(connectionString.concat(";"), m);
        try (Connection c = DriverManager.getConnection(cs)) {
            try (Statement s = c.createStatement()) {
                ResiliencyUtils.killConnection(c, connectionString);
                ResiliencyUtils.blockConnection(c);
                startTime = System.currentTimeMillis();
                s.executeQuery("SELECT 1");
                fail("Successfully executed query on a blocked connection.");
            } catch (SQLException e) {
                double elapsedTime = System.currentTimeMillis() - startTime;
                assertTrue("Query did not timeout in time, elapsed time(ms): " + elapsedTime, elapsedTime < 25000);
                assertTrue("Unexpected Error Message: " + e.getMessage(),
                        e.getMessage().matches(TestUtils.formatErrorMsg("R_crClientAllRecoveryAttemptsFailed")));
            }
        }
    }

    @Test
    public void testRequestRecovery() throws SQLException, IllegalArgumentException, IllegalAccessException, NoSuchMethodException, SecurityException, InvocationTargetException {
        Map<String, String> m = new HashMap<>();
        m.put("connectRetryCount", "1");
        String cs = ResiliencyUtils.setConnectionProps(connectionString.concat(";"), m);

        try (Connection c = DriverManager.getConnection(cs)) {
            Field fields[] = c.getClass().getSuperclass().getDeclaredFields();
            for (Field f : fields) {
                if (f.getName() == "sessionRecovery") {
                    f.setAccessible(true);
                    Object sessionRecoveryFeature = f.get(c);
                    Method method = sessionRecoveryFeature.getClass()
                            .getDeclaredMethod("isConnectionRecoveryNegotiated");
                    method.setAccessible(true);
                    boolean b = (boolean) method.invoke(sessionRecoveryFeature);
                    assertTrue("Session Recovery not negotiated when requested", b);
                }
            }
        }
    }

    @Test
    public void testNoRecovery() throws SQLException, IllegalArgumentException, IllegalAccessException, NoSuchMethodException, SecurityException, InvocationTargetException {
        Map<String, String> m = new HashMap<>();
        m.put("connectRetryCount", "0");
        String cs = ResiliencyUtils.setConnectionProps(connectionString.concat(";"), m);

        try (Connection c = DriverManager.getConnection(cs)) {
            Field fields[] = c.getClass().getSuperclass().getDeclaredFields();
            for (Field f : fields) {
                if (f.getName() == "sessionRecovery") {
                    f.setAccessible(true);
                    Object sessionRecoveryFeature = f.get(c);
                    Method method = sessionRecoveryFeature.getClass()
                            .getDeclaredMethod("isConnectionRecoveryNegotiated");
                    method.setAccessible(true);
                    boolean b = (boolean) method.invoke(sessionRecoveryFeature);
                    assertTrue("Session Recovery recieved when not negotiated", !b);
                }
            }
        }
    }
}
