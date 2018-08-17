/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc;

import java.sql.SQLException;


abstract class SQLServerLob {

    /**
     * Provides functionality for the result set to maintain blobs it has created.
     * 
     * @throws SQLException
     */
    abstract void fillFromStream() throws SQLException;
}
