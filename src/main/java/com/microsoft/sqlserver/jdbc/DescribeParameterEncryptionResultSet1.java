/*
 * Microsoft JDBC Driver for SQL Server
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

/**
* Fields in the first result set of "sp_describe_parameter_encryption"
* We expect the server to return the fields in the result set in the same order as mentioned below.
* If the server changes the below order, then transparent parameter encryption will break.
*/

enum DescribeParameterEncryptionResultSet1 {
    KeyOrdinal,
    DbId,
    KeyId,
    KeyVersion,
    KeyMdVersion,
    EncryptedKey,
    ProviderName,
    KeyPath,
    KeyEncryptionAlgorithm;

    int value() {
        // Column indexing starts from 1;
        return ordinal() + 1;
    }
}