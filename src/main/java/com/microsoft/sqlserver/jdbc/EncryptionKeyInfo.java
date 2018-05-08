/*
 * Microsoft JDBC Driver for SQL Server
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

/**
 * Represents a single encrypted value for a CEK. It contains the encrypted CEK,the store type, name,the key path and encryption algorithm.
 */
class EncryptionKeyInfo {
    EncryptionKeyInfo(byte[] encryptedKeyVal,
            int dbId,
            int keyId,
            int keyVersion,
            byte[] mdVersion,
            String keyPathVal,
            String keyStoreNameVal,
            String algorithmNameVal) {
        encryptedKey = encryptedKeyVal;
        databaseId = dbId;
        cekId = keyId;
        cekVersion = keyVersion;
        cekMdVersion = mdVersion;
        keyPath = keyPathVal;
        keyStoreName = keyStoreNameVal;
        algorithmName = algorithmNameVal;
    }

    byte[] encryptedKey; // the encrypted "column encryption key"
    int databaseId;
    int cekId;
    int cekVersion;
    byte[] cekMdVersion;
    String keyPath;
    String keyStoreName;
    String algorithmName;
    byte normalizationRuleVersion;
}
