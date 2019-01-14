package com.microsoft.sqlserver.jdbc.resiliency;

import static org.junit.Assert.assertTrue;

/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.Test;

import com.microsoft.sqlserver.testframework.AbstractTest;

public class ReflectiveTests extends AbstractTest {
    
    @Test
    public void testNoRecovery() throws SQLException, IllegalArgumentException, IllegalAccessException, NoSuchMethodException, SecurityException, InvocationTargetException {
        Map<String, String> m = new HashMap<>();
        m.put("connectRetryCount", "0");
        String cs = ResiliencyUtils.setConnectionProps(connectionString+";", m);

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
}
