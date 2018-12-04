/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.AlwaysEncrypted;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.LinkedList;

import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;
import org.opentest4j.TestAbortedException;

import com.microsoft.sqlserver.jdbc.RandomData;
import com.microsoft.sqlserver.jdbc.SQLServerConnection;
import com.microsoft.sqlserver.jdbc.SQLServerPreparedStatement;
import com.microsoft.sqlserver.jdbc.SQLServerResultSet;
import com.microsoft.sqlserver.jdbc.SQLServerStatement;
import com.microsoft.sqlserver.jdbc.TestResource;
import com.microsoft.sqlserver.jdbc.TestUtils;
import com.microsoft.sqlserver.testframework.AbstractSQLGenerator;


/**
 * Tests Decryption and encryption of values
 *
 */
@RunWith(JUnitPlatform.class)
public class JDBCEncryptionDecryptionTest extends AESetup {

    private boolean nullable = false;
    private String[] numericValues = null;
    private String[] numericValues2 = null;
    private String[] numericValuesNull = null;
    private String[] numericValuesNull2 = null;
    private String[] charValues = null;

    private LinkedList<byte[]> byteValuesSetObject = null;
    private LinkedList<byte[]> byteValuesNull = null;

    private LinkedList<Object> dateValues = null;

    /**
     * Junit test case for char set string for string values
     * 
     * @throws SQLException
     */
    @Test
    public void testCharSpecificSetter() throws SQLException {
        try (SQLServerConnection con = (SQLServerConnection) DriverManager.getConnection(AETestConnectionString,
                AEInfo); SQLServerStatement stmt = (SQLServerStatement) con.createStatement()) {
            charValues = createCharValues(nullable);
            dropTables(stmt);
            createCharTable();
            populateCharNormalCase(charValues);
            testChar(stmt, charValues);
            testChar(null, charValues);
        }
    }

    /**
     * Junit test case for char set object for string values
     * 
     * @throws SQLException
     */
    @Test
    public void testCharSetObject() throws SQLException {
        try (SQLServerConnection con = (SQLServerConnection) DriverManager.getConnection(AETestConnectionString,
                AEInfo); SQLServerStatement stmt = (SQLServerStatement) con.createStatement()) {
            charValues = createCharValues(nullable);
            dropTables(stmt);
            createCharTable();
            populateCharSetObject(charValues);
            testChar(stmt, charValues);
            testChar(null, charValues);
        }
    }

    /**
     * Junit test case for char set object for jdbc string values
     * 
     * @throws SQLException
     */
    @Test
    public void testCharSetObjectWithJDBCTypes() throws SQLException {

        try (SQLServerConnection con = (SQLServerConnection) DriverManager.getConnection(AETestConnectionString,
                AEInfo); SQLServerStatement stmt = (SQLServerStatement) con.createStatement()) {
            charValues = createCharValues(nullable);
            dropTables(stmt);
            createCharTable();
            populateCharSetObjectWithJDBCTypes(charValues);
            testChar(stmt, charValues);
            testChar(null, charValues);
        }
    }

    /**
     * Junit test case for char set string for null values
     * 
     * @throws SQLException
     */
    @Test
    public void testCharSpecificSetterNull() throws SQLException {
        try (SQLServerConnection con = (SQLServerConnection) DriverManager.getConnection(AETestConnectionString,
                AEInfo); SQLServerStatement stmt = (SQLServerStatement) con.createStatement()) {
            String[] charValuesNull = {null, null, null, null, null, null, null, null, null};
            dropTables(stmt);
            createCharTable();
            populateCharNormalCase(charValuesNull);
            testChar(stmt, charValuesNull);
            testChar(null, charValuesNull);
        }
    }

    /**
     * Junit test case for char set object for null values
     * 
     * @throws SQLException
     */
    @Test
    public void testCharSetObjectNull() throws SQLException {
        try (SQLServerConnection con = (SQLServerConnection) DriverManager.getConnection(AETestConnectionString,
                AEInfo); SQLServerStatement stmt = (SQLServerStatement) con.createStatement()) {
            String[] charValuesNull = {null, null, null, null, null, null, null, null, null};
            dropTables(stmt);
            createCharTable();
            populateCharSetObject(charValuesNull);
            testChar(stmt, charValuesNull);
            testChar(null, charValuesNull);
        }
    }

    /**
     * Junit test case for char set null for null values
     * 
     * @throws SQLException
     */
    @Test
    public void testCharSetNull() throws SQLException {
        try (SQLServerConnection con = (SQLServerConnection) DriverManager.getConnection(AETestConnectionString,
                AEInfo); SQLServerStatement stmt = (SQLServerStatement) con.createStatement()) {
            String[] charValuesNull = {null, null, null, null, null, null, null, null, null};
            dropTables(stmt);
            createCharTable();
            populateCharNullCase();
            testChar(stmt, charValuesNull);
            testChar(null, charValuesNull);
        }
    }

    /**
     * Junit test case for binary set binary for binary values
     * 
     * @throws SQLException
     */
    @Test
    public void testBinarySpecificSetter() throws SQLException {
        try (SQLServerConnection con = (SQLServerConnection) DriverManager.getConnection(AETestConnectionString,
                AEInfo); SQLServerStatement stmt = (SQLServerStatement) con.createStatement()) {
            LinkedList<byte[]> byteValues = createbinaryValues(false);
            dropTables(stmt);
            createBinaryTable();
            populateBinaryNormalCase(byteValues);
            testBinary(stmt, byteValues);
            testBinary(null, byteValues);
        }
    }

    /**
     * Junit test case for binary set object for binary values
     * 
     * @throws SQLException
     */
    @Test
    public void testBinarySetobject() throws SQLException {
        try (SQLServerConnection con = (SQLServerConnection) DriverManager.getConnection(AETestConnectionString,
                AEInfo); SQLServerStatement stmt = (SQLServerStatement) con.createStatement()) {
            byteValuesSetObject = createbinaryValues(false);
            dropTables(stmt);
            createBinaryTable();
            populateBinarySetObject(byteValuesSetObject);
            testBinary(stmt, byteValuesSetObject);
            testBinary(null, byteValuesSetObject);
        }
    }

    /**
     * Junit test case for binary set null for binary values
     * 
     * @throws SQLException
     */
    @Test
    public void testBinarySetNull() throws SQLException {
        try (SQLServerConnection con = (SQLServerConnection) DriverManager.getConnection(AETestConnectionString,
                AEInfo); SQLServerStatement stmt = (SQLServerStatement) con.createStatement()) {
            byteValuesNull = createbinaryValues(true);
            dropTables(stmt);
            createBinaryTable();
            populateBinaryNullCase();
            testBinary(stmt, byteValuesNull);
            testBinary(null, byteValuesNull);
        }
    }

    /**
     * Junit test case for binary set binary for null values
     * 
     * @throws SQLException
     */
    @Test
    public void testBinarySpecificSetterNull() throws SQLException {
        try (SQLServerConnection con = (SQLServerConnection) DriverManager.getConnection(AETestConnectionString,
                AEInfo); SQLServerStatement stmt = (SQLServerStatement) con.createStatement()) {
            byteValuesNull = createbinaryValues(true);
            dropTables(stmt);
            createBinaryTable();
            populateBinaryNormalCase(null);
            testBinary(stmt, byteValuesNull);
            testBinary(null, byteValuesNull);
        }
    }

    /**
     * Junit test case for binary set object for null values
     * 
     * @throws SQLException
     */
    @Test
    public void testBinarysetObjectNull() throws SQLException {
        try (SQLServerConnection con = (SQLServerConnection) DriverManager.getConnection(AETestConnectionString,
                AEInfo); SQLServerStatement stmt = (SQLServerStatement) con.createStatement()) {
            byteValuesNull = createbinaryValues(true);
            dropTables(stmt);
            createBinaryTable();
            populateBinarySetObject(null);
            testBinary(stmt, byteValuesNull);
            testBinary(null, byteValuesNull);
        }
    }

    /**
     * Junit test case for binary set object for jdbc type binary values
     * 
     * @throws SQLException
     */
    @Test
    public void testBinarySetObjectWithJDBCTypes() throws SQLException {

        try (SQLServerConnection con = (SQLServerConnection) DriverManager.getConnection(AETestConnectionString,
                AEInfo); SQLServerStatement stmt = (SQLServerStatement) con.createStatement()) {
            byteValuesSetObject = createbinaryValues(false);
            dropTables(stmt);
            createBinaryTable();
            populateBinarySetObjectWithJDBCType(byteValuesSetObject);
            testBinary(stmt, byteValuesSetObject);
            testBinary(null, byteValuesSetObject);
        }
    }

    /**
     * Junit test case for date set date for date values
     * 
     * @throws SQLException
     */
    @Test
    public void testDateSpecificSetter() throws SQLException {
        try (SQLServerConnection con = (SQLServerConnection) DriverManager.getConnection(AETestConnectionString,
                AEInfo); SQLServerStatement stmt = (SQLServerStatement) con.createStatement()) {
            dateValues = createTemporalTypes(nullable);
            dropTables(stmt);
            createDateTable();
            populateDateNormalCase(dateValues);
            testDate(stmt, dateValues);
            testDate(null, dateValues);
        }
    }

    /**
     * Junit test case for date set object for date values
     * 
     * @throws SQLException
     */
    @Test
    public void testDateSetObject() throws SQLException {
        try (SQLServerConnection con = (SQLServerConnection) DriverManager.getConnection(AETestConnectionString,
                AEInfo); SQLServerStatement stmt = (SQLServerStatement) con.createStatement()) {
            dateValues = createTemporalTypes(nullable);
            dropTables(stmt);
            createDateTable();
            populateDateSetObject(dateValues, "");
            testDate(stmt, dateValues);
            testDate(null, dateValues);
        }
    }

    /**
     * Junit test case for date set object for java date values
     * 
     * @throws SQLException
     */
    @Test
    public void testDateSetObjectWithJavaType() throws SQLException {
        try (SQLServerConnection con = (SQLServerConnection) DriverManager.getConnection(AETestConnectionString,
                AEInfo); SQLServerStatement stmt = (SQLServerStatement) con.createStatement()) {
            dateValues = createTemporalTypes(nullable);
            dropTables(stmt);
            createDateTable();
            populateDateSetObject(dateValues, "setwithJavaType");
            testDate(stmt, dateValues);
            testDate(null, dateValues);
        }
    }

    /**
     * Junit test case for date set object for jdbc date values
     * 
     * @throws SQLException
     */
    @Test
    public void testDateSetObjectWithJDBCType() throws SQLException {
        try (SQLServerConnection con = (SQLServerConnection) DriverManager.getConnection(AETestConnectionString,
                AEInfo); SQLServerStatement stmt = (SQLServerStatement) con.createStatement()) {
            dateValues = createTemporalTypes(nullable);
            dropTables(stmt);
            createDateTable();
            populateDateSetObject(dateValues, "setwithJDBCType");
            testDate(stmt, dateValues);
            testDate(null, dateValues);
        }
    }

    /**
     * Junit test case for date set date for min/max date values
     * 
     * @throws SQLException
     */
    @Test
    public void testDateSpecificSetterMinMaxValue() throws SQLException {
        try (SQLServerConnection con = (SQLServerConnection) DriverManager.getConnection(AETestConnectionString,
                AEInfo); SQLServerStatement stmt = (SQLServerStatement) con.createStatement()) {
            RandomData.returnMinMax = true;
            dateValues = createTemporalTypes(nullable);
            dropTables(stmt);
            createDateTable();
            populateDateNormalCase(dateValues);
            testDate(stmt, dateValues);
            testDate(null, dateValues);
        }
    }

    /**
     * Junit test case for date set date for null values
     * 
     * @throws SQLException
     */
    @Test
    public void testDateSetNull() throws SQLException {
        try (SQLServerConnection con = (SQLServerConnection) DriverManager.getConnection(AETestConnectionString,
                AEInfo); SQLServerStatement stmt = (SQLServerStatement) con.createStatement()) {
            RandomData.returnNull = true;
            nullable = true;

            dateValues = createTemporalTypes(nullable);
            dropTables(stmt);
            createDateTable();
            populateDateNullCase();
            testDate(stmt, dateValues);
            testDate(null, dateValues);
        }

        nullable = false;
        RandomData.returnNull = false;
    }

    /**
     * Junit test case for date set object for null values
     * 
     * @throws SQLException
     */
    @Test
    public void testDateSetObjectNull() throws SQLException {
        RandomData.returnNull = true;
        nullable = true;

        try (SQLServerConnection con = (SQLServerConnection) DriverManager.getConnection(AETestConnectionString,
                AEInfo); SQLServerStatement stmt = (SQLServerStatement) con.createStatement()) {
            dateValues = createTemporalTypes(nullable);
            dropTables(stmt);
            createDateTable();
            populateDateSetObjectNull();
            testDate(stmt, dateValues);
            testDate(null, dateValues);
        }

        nullable = false;
        RandomData.returnNull = false;
    }

    /**
     * Junit test case for numeric set numeric for numeric values
     * 
     * @throws SQLException
     */
    @Test
    public void testNumericSpecificSetter() throws TestAbortedException, Exception {
        numericValues = createNumericValues(nullable);
        numericValues2 = new String[numericValues.length];
        System.arraycopy(numericValues, 0, numericValues2, 0, numericValues.length);

        try (SQLServerConnection con = (SQLServerConnection) DriverManager.getConnection(AETestConnectionString,
                AEInfo); SQLServerStatement stmt = (SQLServerStatement) con.createStatement()) {
            dropTables(stmt);
            createNumericTable();
            populateNumeric(numericValues);
            testNumeric(stmt, numericValues, false);
            testNumeric(null, numericValues2, false);
        }
    }

    /**
     * Junit test case for numeric set object for numeric values
     * 
     * @throws SQLException
     */
    @Test
    public void testNumericSetObject() throws SQLException {
        numericValues = createNumericValues(nullable);
        numericValues2 = new String[numericValues.length];
        System.arraycopy(numericValues, 0, numericValues2, 0, numericValues.length);

        try (SQLServerConnection con = (SQLServerConnection) DriverManager.getConnection(AETestConnectionString,
                AEInfo); SQLServerStatement stmt = (SQLServerStatement) con.createStatement()) {
            dropTables(stmt);
            createNumericTable();
            populateNumericSetObject(numericValues);
            testNumeric(null, numericValues, false);
            testNumeric(stmt, numericValues2, false);
        }
    }

    /**
     * Junit test case for numeric set object for jdbc type numeric values
     * 
     * @throws SQLException
     */
    @Test
    public void testNumericSetObjectWithJDBCTypes() throws SQLException {

        try (SQLServerConnection con = (SQLServerConnection) DriverManager.getConnection(AETestConnectionString,
                AEInfo); SQLServerStatement stmt = (SQLServerStatement) con.createStatement()) {
            numericValues = createNumericValues(nullable);
            numericValues2 = new String[numericValues.length];
            System.arraycopy(numericValues, 0, numericValues2, 0, numericValues.length);

            dropTables(stmt);
            createNumericTable();
            populateNumericSetObjectWithJDBCTypes(numericValues);
            testNumeric(stmt, numericValues, false);
            testNumeric(null, numericValues2, false);
        }
    }

    /**
     * Junit test case for numeric set numeric for max numeric values
     * 
     * @throws SQLException
     */
    @Test
    public void testNumericSpecificSetterMaxValue() throws SQLException {
        String[] numericValuesBoundaryPositive = {"true", "255", "32767", "2147483647", "9223372036854775807",
                "1.79E308", "1.123", "3.4E38", "999999999999999999", "12345.12345", "999999999999999999", "567812.78",
                "214748.3647", "922337203685477.5807", "999999999999999999999999.9999",
                "999999999999999999999999.9999"};
        String[] numericValuesBoundaryPositive2 = {"true", "255", "32767", "2147483647", "9223372036854775807",
                "1.79E308", "1.123", "3.4E38", "999999999999999999", "12345.12345", "999999999999999999", "567812.78",
                "214748.3647", "922337203685477.5807", "999999999999999999999999.9999",
                "999999999999999999999999.9999"};

        try (SQLServerConnection con = (SQLServerConnection) DriverManager.getConnection(AETestConnectionString,
                AEInfo); SQLServerStatement stmt = (SQLServerStatement) con.createStatement()) {
            dropTables(stmt);
            createNumericTable();
            populateNumeric(numericValuesBoundaryPositive);
            testNumeric(stmt, numericValuesBoundaryPositive, false);
            testNumeric(null, numericValuesBoundaryPositive2, false);
        }
    }

    /**
     * Junit test case for numeric set numeric for min numeric values
     * 
     * @throws SQLException
     */
    @Test
    public void testNumericSpecificSetterMinValue() throws SQLException {
        String[] numericValuesBoundaryNegtive = {"false", "0", "-32768", "-2147483648", "-9223372036854775808",
                "-1.79E308", "1.123", "-3.4E38", "999999999999999999", "12345.12345", "999999999999999999", "567812.78",
                "-214748.3648", "-922337203685477.5808", "999999999999999999999999.9999",
                "999999999999999999999999.9999"};
        String[] numericValuesBoundaryNegtive2 = {"false", "0", "-32768", "-2147483648", "-9223372036854775808",
                "-1.79E308", "1.123", "-3.4E38", "999999999999999999", "12345.12345", "999999999999999999", "567812.78",
                "-214748.3648", "-922337203685477.5808", "999999999999999999999999.9999",
                "999999999999999999999999.9999"};

        try (SQLServerConnection con = (SQLServerConnection) DriverManager.getConnection(AETestConnectionString,
                AEInfo); SQLServerStatement stmt = (SQLServerStatement) con.createStatement()) {
            dropTables(stmt);
            createNumericTable();
            populateNumeric(numericValuesBoundaryNegtive);
            testNumeric(stmt, numericValuesBoundaryNegtive, false);
            testNumeric(null, numericValuesBoundaryNegtive2, false);
        }
    }

    /**
     * Junit test case for numeric set numeric for null values
     * 
     * @throws SQLException
     */
    @Test
    public void testNumericSpecificSetterNull() throws SQLException {
        nullable = true;
        RandomData.returnNull = true;
        numericValuesNull = createNumericValues(nullable);
        numericValuesNull2 = new String[numericValuesNull.length];
        System.arraycopy(numericValuesNull, 0, numericValuesNull2, 0, numericValuesNull.length);

        try (SQLServerConnection con = (SQLServerConnection) DriverManager.getConnection(AETestConnectionString,
                AEInfo); SQLServerStatement stmt = (SQLServerStatement) con.createStatement()) {
            dropTables(stmt);
            createNumericTable();
            populateNumericNullCase(numericValuesNull);
            testNumeric(stmt, numericValuesNull, true);
            testNumeric(null, numericValuesNull2, true);
        }

        nullable = false;
        RandomData.returnNull = false;
    }

    /**
     * Junit test case for numeric set object for null values
     * 
     * @throws SQLException
     */
    @Test
    public void testNumericSpecificSetterSetObjectNull() throws SQLException {
        nullable = true;
        RandomData.returnNull = true;
        numericValuesNull = createNumericValues(nullable);
        numericValuesNull2 = new String[numericValuesNull.length];
        System.arraycopy(numericValuesNull, 0, numericValuesNull2, 0, numericValuesNull.length);

        try (SQLServerConnection con = (SQLServerConnection) DriverManager.getConnection(AETestConnectionString,
                AEInfo); SQLServerStatement stmt = (SQLServerStatement) con.createStatement()) {
            dropTables(stmt);
            createNumericTable();
            populateNumericSetObjectNull();
            testNumeric(stmt, numericValuesNull, true);
            testNumeric(null, numericValuesNull2, true);
        }

        nullable = false;
        RandomData.returnNull = false;
    }

    /**
     * Junit test case for numeric set numeric for null normalization values
     * 
     * @throws SQLException
     */
    @Test
    public void testNumericNormalization() throws SQLException {
        String[] numericValuesNormalization = {"true", "1", "127", "100", "100", "1.123", "1.123", "1.123",
                "123456789123456789", "12345.12345", "987654321123456789", "567812.78", "7812.7812", "7812.7812",
                "999999999999999999999999.9999", "999999999999999999999999.9999"};
        String[] numericValuesNormalization2 = {"true", "1", "127", "100", "100", "1.123", "1.123", "1.123",
                "123456789123456789", "12345.12345", "987654321123456789", "567812.78", "7812.7812", "7812.7812",
                "999999999999999999999999.9999", "999999999999999999999999.9999"};

        try (SQLServerConnection con = (SQLServerConnection) DriverManager.getConnection(AETestConnectionString,
                AEInfo); SQLServerStatement stmt = (SQLServerStatement) con.createStatement()) {
            dropTables(stmt);
            createNumericTable();
            populateNumericNormalCase(numericValuesNormalization);
            testNumeric(stmt, numericValuesNormalization, false);
            testNumeric(null, numericValuesNormalization2, false);
        }
    }

    private void testChar(SQLServerStatement stmt, String[] values) throws SQLException {
        String sql = "select * from " + AbstractSQLGenerator.escapeIdentifier(charTable);
        try (SQLServerConnection con = (SQLServerConnection) DriverManager.getConnection(AETestConnectionString,
                AEInfo);
                SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) TestUtils.getPreparedStmt(con, sql,
                        stmtColEncSetting)) {
            try (ResultSet rs = (stmt == null) ? pstmt.executeQuery() : stmt.executeQuery(sql)) {
                int numberOfColumns = rs.getMetaData().getColumnCount();
                while (rs.next()) {
                    testGetString(rs, numberOfColumns, values);
                    testGetObject(rs, numberOfColumns, values);
                }
            }
        }

    }

    private void testBinary(SQLServerStatement stmt, LinkedList<byte[]> values) throws SQLException {
        String sql = "select * from " + AbstractSQLGenerator.escapeIdentifier(binaryTable);

        try (SQLServerConnection con = (SQLServerConnection) DriverManager.getConnection(AETestConnectionString,
                AEInfo);
                SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) TestUtils.getPreparedStmt(con, sql,
                        stmtColEncSetting)) {
            try (ResultSet rs = (stmt == null) ? pstmt.executeQuery() : stmt.executeQuery(sql)) {
                int numberOfColumns = rs.getMetaData().getColumnCount();
                while (rs.next()) {
                    testGetStringForBinary(rs, numberOfColumns, values);
                    testGetBytes(rs, numberOfColumns, values);
                    testGetObjectForBinary(rs, numberOfColumns, values);
                }
            }
        }
    }

    private void testDate(SQLServerStatement stmt, LinkedList<Object> values1) throws SQLException {
        String sql = "select * from " + AbstractSQLGenerator.escapeIdentifier(dateTable);

        try (SQLServerConnection con = (SQLServerConnection) DriverManager.getConnection(AETestConnectionString,
                AEInfo);
                SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) TestUtils.getPreparedStmt(con, sql,
                        stmtColEncSetting)) {
            try (ResultSet rs = (stmt == null) ? pstmt.executeQuery() : stmt.executeQuery(sql)) {
                int numberOfColumns = rs.getMetaData().getColumnCount();
                while (rs.next()) {
                    // testGetStringForDate(rs, numberOfColumns, values1); //TODO: Disabling, since getString throws
                    // verification error for zero temporal
                    // types
                    testGetObjectForTemporal(rs, numberOfColumns, values1);
                    testGetDate(rs, numberOfColumns, values1);
                }
            }
        }
    }

    private void testGetObject(ResultSet rs, int numberOfColumns, String[] values) throws SQLException {
        int index = 0;
        for (int i = 1; i <= numberOfColumns; i = i + 3) {
            try {
                String objectValue1 = ("" + rs.getObject(i)).trim();
                String objectValue2 = ("" + rs.getObject(i + 1)).trim();
                String objectValue3 = ("" + rs.getObject(i + 2)).trim();

                boolean matches = objectValue1.equalsIgnoreCase("" + values[index])
                        && objectValue2.equalsIgnoreCase("" + values[index])
                        && objectValue3.equalsIgnoreCase("" + values[index]);

                if (("" + values[index]).length() >= 1000) {
                    assertTrue(matches,
                            TestResource.getResource("R_decryptionFailed") + "getObject(): " + i + ", " + (i + 1) + ", "
                                    + (i + 2) + ".\n" + TestResource.getResource("R_expectedValueAtIndex") + index);
                } else {
                    assertTrue(matches,
                            TestResource.getResource("R_decryptionFailed") + "getObject(): " + objectValue1 + ", "
                                    + objectValue2 + ", " + objectValue3 + ".\n"
                                    + TestResource.getResource("R_expectedValue") + values[index]);
                }
            } finally {
                index++;
            }
        }
    }

    private void testGetObjectForTemporal(ResultSet rs, int numberOfColumns,
            LinkedList<Object> values) throws SQLException {
        int index = 0;
        for (int i = 1; i <= numberOfColumns; i = i + 3) {
            try {
                String objectValue1 = ("" + rs.getObject(i)).trim();
                String objectValue2 = ("" + rs.getObject(i + 1)).trim();
                String objectValue3 = ("" + rs.getObject(i + 2)).trim();

                Object expected = null;
                if (rs.getMetaData().getColumnTypeName(i).equalsIgnoreCase("smalldatetime")) {
                    expected = TestUtils.roundSmallDateTimeValue(values.get(index));
                } else if (rs.getMetaData().getColumnTypeName(i).equalsIgnoreCase("datetime")) {
                    expected = TestUtils.roundDatetimeValue(values.get(index));
                } else {
                    expected = values.get(index);
                }
                assertTrue(
                        objectValue1.equalsIgnoreCase("" + expected) && objectValue2.equalsIgnoreCase("" + expected)
                                && objectValue3.equalsIgnoreCase("" + expected),
                        TestResource.getResource("R_decryptionFailed") + "getObject(): " + objectValue1 + ", "
                                + objectValue2 + ", " + objectValue3 + ".\n"
                                + TestResource.getResource("R_expectedValue") + expected);
            } finally {
                index++;
            }
        }
    }

    private void testGetObjectForBinary(ResultSet rs, int numberOfColumns,
            LinkedList<byte[]> values) throws SQLException {
        int index = 0;
        for (int i = 1; i <= numberOfColumns; i = i + 3) {
            byte[] objectValue1 = (byte[]) rs.getObject(i);
            byte[] objectValue2 = (byte[]) rs.getObject(i + 1);
            byte[] objectValue3 = (byte[]) rs.getObject(i + 2);

            byte[] expectedBytes = null;

            if (null != values.get(index)) {
                expectedBytes = values.get(index);
            }

            try {
                if (null != values.get(index)) {
                    for (int j = 0; j < expectedBytes.length; j++) {
                        assertTrue(
                                expectedBytes[j] == objectValue1[j] && expectedBytes[j] == objectValue2[j]
                                        && expectedBytes[j] == objectValue3[j],
                                TestResource.getResource("R_decryptionFailed") + "getObject(): " + objectValue1 + ", "
                                        + objectValue2 + ", " + objectValue3 + ".\n");
                    }
                }
            } finally {
                index++;
            }
        }
    }

    private void testGetBigDecimal(ResultSet rs, int numberOfColumns, String[] values) throws SQLException {

        int index = 0;
        for (int i = 1; i <= numberOfColumns; i = i + 3) {

            String decimalValue1 = "" + rs.getBigDecimal(i);
            String decimalValue2 = "" + rs.getBigDecimal(i + 1);
            String decimalValue3 = "" + rs.getBigDecimal(i + 2);

            if (decimalValue1.equalsIgnoreCase("0")
                    && (values[index].equalsIgnoreCase("true") || values[index].equalsIgnoreCase("false"))) {
                decimalValue1 = "false";
                decimalValue2 = "false";
                decimalValue3 = "false";
            } else if (decimalValue1.equalsIgnoreCase("1")
                    && (values[index].equalsIgnoreCase("true") || values[index].equalsIgnoreCase("false"))) {
                decimalValue1 = "true";
                decimalValue2 = "true";
                decimalValue3 = "true";
            }

            if (null != values[index]) {
                if (values[index].equalsIgnoreCase("1.79E308")) {
                    values[index] = "1.79E+308";
                } else if (values[index].equalsIgnoreCase("3.4E38")) {
                    values[index] = "3.4E+38";
                }

                if (values[index].equalsIgnoreCase("-1.79E308")) {
                    values[index] = "-1.79E+308";
                } else if (values[index].equalsIgnoreCase("-3.4E38")) {
                    values[index] = "-3.4E+38";
                }
            }

            try {
                assertTrue(
                        decimalValue1.equalsIgnoreCase("" + values[index])
                                && decimalValue2.equalsIgnoreCase("" + values[index])
                                && decimalValue3.equalsIgnoreCase("" + values[index]),
                        TestResource.getResource("R_decryptionFailed") + "getBigDecimal(): " + decimalValue1 + ", "
                                + decimalValue2 + ", " + decimalValue3 + ".\n"
                                + TestResource.getResource("R_expectedValue") + values[index]);
            } finally {
                index++;
            }
        }
    }

    private void testGetString(ResultSet rs, int numberOfColumns, String[] values) throws SQLException {

        int index = 0;
        for (int i = 1; i <= numberOfColumns; i = i + 3) {
            String stringValue1 = ("" + rs.getString(i)).trim();
            String stringValue2 = ("" + rs.getString(i + 1)).trim();
            String stringValue3 = ("" + rs.getString(i + 2)).trim();

            if (stringValue1.equalsIgnoreCase("0")
                    && (values[index].equalsIgnoreCase("true") || values[index].equalsIgnoreCase("false"))) {
                stringValue1 = "false";
                stringValue2 = "false";
                stringValue3 = "false";
            } else if (stringValue1.equalsIgnoreCase("1")
                    && (values[index].equalsIgnoreCase("true") || values[index].equalsIgnoreCase("false"))) {
                stringValue1 = "true";
                stringValue2 = "true";
                stringValue3 = "true";
            }
            try {

                boolean matches = stringValue1.equalsIgnoreCase("" + values[index])
                        && stringValue2.equalsIgnoreCase("" + values[index])
                        && stringValue3.equalsIgnoreCase("" + values[index]);

                if (("" + values[index]).length() >= 1000) {
                    assertTrue(matches, TestResource.getResource("R_decryptionFailed") + "getString():" + i + ", "
                            + (i + 1) + ", " + (i + 2) + ".\n" + TestResource.getResource("R_expectedValue") + index);
                } else {
                    assertTrue(matches,
                            TestResource.getResource("R_decryptionFailed") + "getString(): " + stringValue1 + ", "
                                    + stringValue2 + ", " + stringValue3 + ".\n"
                                    + TestResource.getResource("R_expectedValue") + values[index]);
                }
            } finally {
                index++;
            }
        }
    }

    // not testing this for now.
    @SuppressWarnings("unused")
    private void testGetStringForDate(ResultSet rs, int numberOfColumns,
            LinkedList<Object> values) throws SQLException {

        int index = 0;
        for (int i = 1; i <= numberOfColumns; i = i + 3) {
            String stringValue1 = ("" + rs.getString(i)).trim();
            String stringValue2 = ("" + rs.getString(i + 1)).trim();
            String stringValue3 = ("" + rs.getString(i + 2)).trim();

            try {
                if (index == 3) {
                    assertTrue(
                            stringValue1.contains("" + values.get(index))
                                    && stringValue2.contains("" + values.get(index))
                                    && stringValue3.contains("" + values.get(index)),
                            TestResource.getResource("R_decryptionFailed") + "getString(): " + stringValue1 + ", "
                                    + stringValue2 + ", " + stringValue3 + ".\n"
                                    + TestResource.getResource("R_expectedValue") + values.get(index));
                } else if (index == 4) // round value for datetime
                {
                    Object datetimeValue = "" + TestUtils.roundDatetimeValue(values.get(index));
                    assertTrue(
                            stringValue1.equalsIgnoreCase("" + datetimeValue)
                                    && stringValue2.equalsIgnoreCase("" + datetimeValue)
                                    && stringValue3.equalsIgnoreCase("" + datetimeValue),
                            TestResource.getResource("R_decryptionFailed") + "getString(): " + stringValue1 + ", "
                                    + stringValue2 + ", " + stringValue3 + ".\n"
                                    + TestResource.getResource("R_expectedValue") + datetimeValue);
                } else if (index == 5) // round value for smalldatetime
                {
                    Object smalldatetimeValue = "" + TestUtils.roundSmallDateTimeValue(values.get(index));
                    assertTrue(
                            stringValue1.equalsIgnoreCase("" + smalldatetimeValue)
                                    && stringValue2.equalsIgnoreCase("" + smalldatetimeValue)
                                    && stringValue3.equalsIgnoreCase("" + smalldatetimeValue),
                            TestResource.getResource("R_decryptionFailed") + "getString(): " + stringValue1 + ", "
                                    + stringValue2 + ", " + stringValue3 + ".\n"
                                    + TestResource.getResource("R_expectedValue") + smalldatetimeValue);
                } else {
                    assertTrue(
                            stringValue1.contains("" + values.get(index))
                                    && stringValue2.contains("" + values.get(index))
                                    && stringValue3.contains("" + values.get(index)),
                            TestResource.getResource("R_decryptionFailed") + "getString(): " + stringValue1 + ", "
                                    + stringValue2 + ", " + stringValue3 + ".\n"
                                    + TestResource.getResource("R_expectedValue") + values.get(index));
                }
            } finally {
                index++;
            }
        }
    }

    private void testGetBytes(ResultSet rs, int numberOfColumns, LinkedList<byte[]> values) throws SQLException {
        int index = 0;
        for (int i = 1; i <= numberOfColumns; i = i + 3) {
            byte[] b1 = rs.getBytes(i);
            byte[] b2 = rs.getBytes(i + 1);
            byte[] b3 = rs.getBytes(i + 2);

            byte[] expectedBytes = null;

            if (null != values.get(index)) {
                expectedBytes = values.get(index);
            }

            try {
                if (null != values.get(index)) {
                    for (int j = 0; j < expectedBytes.length; j++) {
                        assertTrue(expectedBytes[j] == b1[j] && expectedBytes[j] == b2[j] && expectedBytes[j] == b3[j],
                                TestResource.getResource("R_decryptionFailed") + "getObject(): " + b1 + ", " + b2 + ", "
                                        + b3 + ".\n");
                    }
                }
            } finally {
                index++;
            }
        }
    }

    private void testGetStringForBinary(ResultSet rs, int numberOfColumns,
            LinkedList<byte[]> values) throws SQLException {

        int index = 0;
        for (int i = 1; i <= numberOfColumns; i = i + 3) {
            String stringValue1 = ("" + rs.getString(i)).trim();
            String stringValue2 = ("" + rs.getString(i + 1)).trim();
            String stringValue3 = ("" + rs.getString(i + 2)).trim();

            StringBuffer expected = new StringBuffer();
            String expectedStr = null;

            if (null != values.get(index)) {
                for (byte b : values.get(index)) {
                    expected.append(String.format("%02X", b));
                }
                expectedStr = "" + expected.toString();
            } else {
                expectedStr = "null";
            }

            try {
                assertTrue(
                        stringValue1.startsWith(expectedStr) && stringValue2.startsWith(expectedStr)
                                && stringValue3.startsWith(expectedStr),
                        TestResource.getResource("R_decryptionFailed") + "getString(): " + stringValue1 + ", "
                                + stringValue2 + ", " + stringValue3 + ".\n"
                                + TestResource.getResource("R_expectedValue") + expectedStr);
            } finally {
                index++;
            }
        }
    }

    private void testGetDate(ResultSet rs, int numberOfColumns, LinkedList<Object> values) throws SQLException {
        for (int i = 1; i <= numberOfColumns; i = i + 3) {

            if (rs instanceof SQLServerResultSet) {

                String stringValue1 = null;
                String stringValue2 = null;
                String stringValue3 = null;
                String expected = null;

                switch (i) {

                    case 1:
                        stringValue1 = "" + ((SQLServerResultSet) rs).getDate(i);
                        stringValue2 = "" + ((SQLServerResultSet) rs).getDate(i + 1);
                        stringValue3 = "" + ((SQLServerResultSet) rs).getDate(i + 2);
                        expected = "" + values.get(0);
                        break;

                    case 4:
                        stringValue1 = "" + ((SQLServerResultSet) rs).getTimestamp(i);
                        stringValue2 = "" + ((SQLServerResultSet) rs).getTimestamp(i + 1);
                        stringValue3 = "" + ((SQLServerResultSet) rs).getTimestamp(i + 2);
                        expected = "" + values.get(1);
                        break;

                    case 7:
                        stringValue1 = "" + ((SQLServerResultSet) rs).getDateTimeOffset(i);
                        stringValue2 = "" + ((SQLServerResultSet) rs).getDateTimeOffset(i + 1);
                        stringValue3 = "" + ((SQLServerResultSet) rs).getDateTimeOffset(i + 2);
                        expected = "" + values.get(2);
                        break;

                    case 10:
                        stringValue1 = "" + ((SQLServerResultSet) rs).getTime(i);
                        stringValue2 = "" + ((SQLServerResultSet) rs).getTime(i + 1);
                        stringValue3 = "" + ((SQLServerResultSet) rs).getTime(i + 2);
                        expected = "" + values.get(3);
                        break;

                    case 13:
                        stringValue1 = "" + ((SQLServerResultSet) rs).getDateTime(i);
                        stringValue2 = "" + ((SQLServerResultSet) rs).getDateTime(i + 1);
                        stringValue3 = "" + ((SQLServerResultSet) rs).getDateTime(i + 2);
                        expected = "" + TestUtils.roundDatetimeValue(values.get(4));
                        break;

                    case 16:
                        stringValue1 = "" + ((SQLServerResultSet) rs).getSmallDateTime(i);
                        stringValue2 = "" + ((SQLServerResultSet) rs).getSmallDateTime(i + 1);
                        stringValue3 = "" + ((SQLServerResultSet) rs).getSmallDateTime(i + 2);
                        expected = "" + TestUtils.roundSmallDateTimeValue(values.get(5));
                        break;

                    default:
                        fail(TestResource.getResource("R_switchFailed"));
                }

                assertTrue(
                        stringValue1.equalsIgnoreCase(expected) && stringValue2.equalsIgnoreCase(expected)
                                && stringValue3.equalsIgnoreCase(expected),
                        TestResource.getResource("R_decryptionFailed") + "testGetDate: " + stringValue1 + ", "
                                + stringValue2 + ", " + stringValue3 + ".\n"
                                + TestResource.getResource("R_expectedValue") + expected);
            }

            else {
                fail(TestResource.getResource("R_resultsetNotInstance"));
            }
        }
    }

    private void testNumeric(Statement stmt, String[] numericValues, boolean isNull) throws SQLException {
        String sql = "select * from " + AbstractSQLGenerator.escapeIdentifier(numericTable);

        try (SQLServerConnection con = (SQLServerConnection) DriverManager.getConnection(AETestConnectionString,
                AEInfo);
                SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) TestUtils.getPreparedStmt(con, sql,
                        stmtColEncSetting)) {
            try (SQLServerResultSet rs = (stmt == null) ? (SQLServerResultSet) pstmt.executeQuery()
                                                        : (SQLServerResultSet) stmt.executeQuery(sql)) {
                int numberOfColumns = rs.getMetaData().getColumnCount();
                while (rs.next()) {
                    testGetString(rs, numberOfColumns, numericValues);
                    testGetObject(rs, numberOfColumns, numericValues);
                    testGetBigDecimal(rs, numberOfColumns, numericValues);
                    if (!isNull)
                        testWithSpecifiedtype(rs, numberOfColumns, numericValues);
                    else {
                        String[] nullNumericValues = {"false", "0", "0", "0", "0", "0.0", "0.0", "0.0", null, null,
                                null, null, null, null, null, null};
                        testWithSpecifiedtype(rs, numberOfColumns, nullNumericValues);
                    }
                }
            }
        }
    }

    private void testWithSpecifiedtype(SQLServerResultSet rs, int numberOfColumns,
            String[] values) throws SQLException {

        String value1, value2, value3, expectedValue = null;
        int index = 0;

        // bit
        value1 = "" + rs.getBoolean(1);
        value2 = "" + rs.getBoolean(2);
        value3 = "" + rs.getBoolean(3);

        expectedValue = values[index];
        Compare(expectedValue, value1, value2, value3);
        index++;

        // tiny
        value1 = "" + rs.getShort(4);
        value2 = "" + rs.getShort(5);
        value3 = "" + rs.getShort(6);

        expectedValue = values[index];
        Compare(expectedValue, value1, value2, value3);
        index++;

        // smallint
        value1 = "" + rs.getShort(7);
        value2 = "" + rs.getShort(8);
        value3 = "" + rs.getShort(8);

        expectedValue = values[index];
        Compare(expectedValue, value1, value2, value3);
        index++;

        // int
        value1 = "" + rs.getInt(10);
        value2 = "" + rs.getInt(11);
        value3 = "" + rs.getInt(12);

        expectedValue = values[index];
        Compare(expectedValue, value1, value2, value3);
        index++;

        // bigint
        value1 = "" + rs.getLong(13);
        value2 = "" + rs.getLong(14);
        value3 = "" + rs.getLong(15);

        expectedValue = values[index];
        Compare(expectedValue, value1, value2, value3);
        index++;

        // float
        value1 = "" + rs.getDouble(16);
        value2 = "" + rs.getDouble(17);
        value3 = "" + rs.getDouble(18);

        expectedValue = values[index];
        Compare(expectedValue, value1, value2, value3);
        index++;

        // float(30)
        value1 = "" + rs.getDouble(19);
        value2 = "" + rs.getDouble(20);
        value3 = "" + rs.getDouble(21);

        expectedValue = values[index];
        Compare(expectedValue, value1, value2, value3);
        index++;

        // real
        value1 = "" + rs.getFloat(22);
        value2 = "" + rs.getFloat(23);
        value3 = "" + rs.getFloat(24);

        expectedValue = values[index];
        Compare(expectedValue, value1, value2, value3);
        index++;

        // decimal
        value1 = "" + rs.getBigDecimal(25);
        value2 = "" + rs.getBigDecimal(26);
        value3 = "" + rs.getBigDecimal(27);

        expectedValue = values[index];
        Compare(expectedValue, value1, value2, value3);
        index++;

        // decimal (10,5)
        value1 = "" + rs.getBigDecimal(28);
        value2 = "" + rs.getBigDecimal(29);
        value3 = "" + rs.getBigDecimal(30);

        expectedValue = values[index];
        Compare(expectedValue, value1, value2, value3);
        index++;

        // numeric
        value1 = "" + rs.getBigDecimal(31);
        value2 = "" + rs.getBigDecimal(32);
        value3 = "" + rs.getBigDecimal(33);

        expectedValue = values[index];
        Compare(expectedValue, value1, value2, value3);
        index++;

        // numeric (8,2)
        value1 = "" + rs.getBigDecimal(34);
        value2 = "" + rs.getBigDecimal(35);
        value3 = "" + rs.getBigDecimal(36);

        expectedValue = values[index];
        Compare(expectedValue, value1, value2, value3);
        index++;

        // smallmoney
        value1 = "" + rs.getSmallMoney(37);
        value2 = "" + rs.getSmallMoney(38);
        value3 = "" + rs.getSmallMoney(39);

        expectedValue = values[index];
        Compare(expectedValue, value1, value2, value3);
        index++;

        // money
        value1 = "" + rs.getMoney(40);
        value2 = "" + rs.getMoney(41);
        value3 = "" + rs.getMoney(42);

        expectedValue = values[index];
        Compare(expectedValue, value1, value2, value3);
        index++;

        // decimal(28,4)
        value1 = "" + rs.getBigDecimal(43);
        value2 = "" + rs.getBigDecimal(44);
        value3 = "" + rs.getBigDecimal(45);

        expectedValue = values[index];
        Compare(expectedValue, value1, value2, value3);
        index++;

        // numeric(28,4)
        value1 = "" + rs.getBigDecimal(46);
        value2 = "" + rs.getBigDecimal(47);
        value3 = "" + rs.getBigDecimal(48);

        expectedValue = values[index];
        Compare(expectedValue, value1, value2, value3);
        index++;
    }

    private void Compare(String expectedValue, String value1, String value2, String value3) {

        if (null != expectedValue) {
            if (expectedValue.equalsIgnoreCase("1.79E+308")) {
                expectedValue = "1.79E308";
            } else if (expectedValue.equalsIgnoreCase("3.4E+38")) {
                expectedValue = "3.4E38";
            }

            if (expectedValue.equalsIgnoreCase("-1.79E+308")) {
                expectedValue = "-1.79E308";
            } else if (expectedValue.equalsIgnoreCase("-3.4E+38")) {
                expectedValue = "-3.4E38";
            }
        }

        assertTrue(
                value1.equalsIgnoreCase("" + expectedValue) && value2.equalsIgnoreCase("" + expectedValue)
                        && value3.equalsIgnoreCase("" + expectedValue),
                TestResource.getResource("R_decryptionFailed") + "getBigDecimal(): " + value1 + ", " + value2 + ", "
                        + value3 + ".\n" + TestResource.getResource("R_expectedValue"));
    }

}
