/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.JDBCType;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;

import com.microsoft.sqlserver.testframework.DBConnection;
import com.microsoft.sqlserver.testframework.DBResultSet;
import com.microsoft.sqlserver.testframework.DBStatement;
import com.microsoft.sqlserver.testframework.DBTable;


public class ComparisonUtil {

    /**
     * test if source table and destination table are the same
     * 
     * @param con
     * @param srcTable
     * @param destTable
     * @throws SQLException
     */
    public static void compareSrcTableAndDestTableIgnoreRowOrder(DBConnection con, DBTable srcTable,
            DBTable destTable) throws SQLException {
        try (DBStatement stmt = con.createStatement(); DBStatement stmt2 = con.createStatement();
                DBResultSet srcResultSetCount = stmt
                        .executeQuery("SELECT COUNT(*) FROM " + srcTable.getEscapedTableName() + ";");
                DBResultSet dstResultSetCount = stmt2
                        .executeQuery("SELECT COUNT(*) FROM " + destTable.getEscapedTableName() + ";")) {
            srcResultSetCount.next();
            dstResultSetCount.next();
            int srcRows = srcResultSetCount.getInt(1);
            int destRows = dstResultSetCount.getInt(1);

            if (srcRows != destRows) {
                fail("Souce table and Destination table have different number of rows.");
            }

            if (srcTable.getColumns().size() != destTable.getColumns().size()) {
                fail("Souce table and Destination table have different number of columns.");
            }

            try (DBResultSet srcResultSet = stmt.executeQuery(
                    "SELECT * FROM " + srcTable.getEscapedTableName() + " ORDER BY [" + srcTable.getColumnName(1)
                            + "], [" + srcTable.getColumnName(2) + "],[" + srcTable.getColumnName(3) + "];");
                    DBResultSet dstResultSet = stmt2.executeQuery("SELECT * FROM " + destTable.getEscapedTableName()
                            + " ORDER BY [" + destTable.getColumnName(1) + "], [" + destTable.getColumnName(2) + "],["
                            + destTable.getColumnName(3) + "];")) {

                while (srcResultSet.next() && dstResultSet.next()) {
                    for (int i = 0; i < destTable.getColumns().size(); i++) {
                        SQLServerResultSetMetaData srcMeta = (SQLServerResultSetMetaData) ((ResultSet) srcResultSet
                                .product()).getMetaData();
                        SQLServerResultSetMetaData destMeta = (SQLServerResultSetMetaData) ((ResultSet) dstResultSet
                                .product()).getMetaData();

                        int srcJDBCTypeInt = srcMeta.getColumnType(i + 1);
                        int destJDBCTypeInt = destMeta.getColumnType(i + 1);

                        // verify column types
                        if (srcJDBCTypeInt != destJDBCTypeInt) {
                            fail("Souce table and Destination table have different number of columns.");
                        }

                        Object expectedValue = srcResultSet.getObject(i + 1);
                        Object actualValue = dstResultSet.getObject(i + 1);

                        compareExpectedAndActual(destJDBCTypeInt, expectedValue, actualValue);
                    }
                }
            }
        }
    }

    /**
     * validate if both expected and actual value are same
     * 
     * @param dataType
     * @param expectedValue
     * @param actualValue
     */
    public static void compareExpectedAndActual(int dataType, Object expectedValue, Object actualValue) {
        // Bulkcopy doesn't guarantee order of insertion - if we need to test several rows either use primary key or
        // validate result based on sql JOIN

        if ((null == expectedValue) || (null == actualValue)) {
            // if one value is null other should be null too
            assertEquals(expectedValue, actualValue, "Expected null in source and destination");
        } else
            switch (dataType) {
                case java.sql.Types.BIGINT:
                    assertTrue((((Long) expectedValue).longValue() == ((Long) actualValue).longValue()),
                            "Unexpected bigint value. Expected:" + ((Long) expectedValue).longValue() + " Actual:"
                                    + ((Long) actualValue).longValue());
                    break;

                case java.sql.Types.INTEGER:
                    assertTrue((((Integer) expectedValue).intValue() == ((Integer) actualValue).intValue()),
                            "Unexpected int value. Expected:" + ((Integer) expectedValue).intValue() + " Actual:"
                                    + ((Integer) actualValue).intValue());
                    break;

                case java.sql.Types.SMALLINT:
                case java.sql.Types.TINYINT:
                    assertTrue((((Short) expectedValue).shortValue() == ((Short) actualValue).shortValue()),
                            "Unexpected smallint/tinyint value. Expected:" + ((Short) expectedValue).shortValue()
                                    + " Actual:" + ((Short) actualValue).shortValue());
                    break;

                case java.sql.Types.BIT:
                    assertTrue((((Boolean) expectedValue).booleanValue() == ((Boolean) actualValue).booleanValue()),
                            "Unexpected bit value");
                    break;

                case java.sql.Types.DECIMAL:
                case java.sql.Types.NUMERIC:
                    assertTrue(0 == (((BigDecimal) expectedValue).compareTo((BigDecimal) actualValue)),
                            "Unexpected decimal/numeric/money/smallmoney value");
                    break;

                case java.sql.Types.DOUBLE:
                    assertTrue((((Double) expectedValue).doubleValue() == ((Double) actualValue).doubleValue()),
                            "Unexpected double value. Expected:" + ((Double) expectedValue).doubleValue() + " Actual:"
                                    + ((Double) actualValue).doubleValue());
                    break;

                case java.sql.Types.REAL:
                    assertTrue((((Float) expectedValue).floatValue() == ((Float) actualValue).floatValue()),
                            "Unexpected real/float value. Expected:" + ((Float) expectedValue).floatValue() + " Actual:"
                                    + ((Float) actualValue).floatValue());
                    break;

                case java.sql.Types.VARCHAR:
                case java.sql.Types.NVARCHAR:
                    assertTrue(((((String) expectedValue).trim()).equals(((String) actualValue).trim())),
                            "Unexpected varchar/nvarchar value ");
                    break;

                case java.sql.Types.CHAR:
                case java.sql.Types.NCHAR:
                    assertTrue(((((String) expectedValue).trim()).equals(((String) actualValue).trim())),
                            "Unexpected char/nchar value ");
                    break;

                case java.sql.Types.BINARY:
                case java.sql.Types.VARBINARY:
                    assertTrue(TestUtils.parseByte((byte[]) expectedValue, (byte[]) actualValue),
                            "Unexpected bianry/varbinary value ");
                    break;

                case java.sql.Types.TIMESTAMP:
                    assertTrue((((Timestamp) expectedValue).getTime() == (((Timestamp) actualValue).getTime())),
                            "Unexpected datetime/smalldatetime/datetime2 value");
                    break;

                case java.sql.Types.DATE:
                    Calendar expC = Calendar.getInstance();
                    expC.setTime((Date) expectedValue);
                    Calendar actC = Calendar.getInstance();
                    actC.setTime((Date) actualValue);
                    assertTrue(expC.get(Calendar.DAY_OF_MONTH) == actC.get(Calendar.DAY_OF_MONTH),
                            "Unexpected datetime value");
                    break;

                case java.sql.Types.TIME:
                    assertTrue(((Time) expectedValue).getTime() == ((Time) actualValue).getTime(),
                            "Unexpected time value ");
                    break;

                case microsoft.sql.Types.DATETIMEOFFSET:
                    assertTrue(0 == ((microsoft.sql.DateTimeOffset) expectedValue)
                            .compareTo((microsoft.sql.DateTimeOffset) actualValue), "Unexpected time value ");
                    break;

                default:
                    fail("Unhandled JDBCType " + JDBCType.valueOf(dataType));
                    break;
            }
    }
}
