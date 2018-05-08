/*
 * Microsoft JDBC Driver for SQL Server
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

import java.util.ArrayList;
import java.util.List;

/**
 * Represents a unique CEK as an entry in the CekTable. A unique (plaintext is unique) CEK can have multiple encrypted CEKs when using multiple CMKs.
 * These encrypted CEKs are represented by a member ArrayList.
 */
class CekTableEntry {
    static final private java.util.logging.Logger aeLogger = java.util.logging.Logger.getLogger("com.microsoft.sqlserver.jdbc.AE");

    List<EncryptionKeyInfo> columnEncryptionKeyValues;
    int ordinal;
    int databaseId;
    int cekId;
    int cekVersion;
    byte[] cekMdVersion;

    List<EncryptionKeyInfo> getColumnEncryptionKeyValues() {
        return columnEncryptionKeyValues;
    }

    int getOrdinal() {
        return ordinal;
    }

    int getDatabaseId() {
        return databaseId;
    }

    int getCekId() {
        return cekId;
    }

    int getCekVersion() {
        return cekVersion;
    }

    byte[] getCekMdVersion() {
        return cekMdVersion;
    }

    CekTableEntry(int ordinalVal) {
        ordinal = ordinalVal;
        databaseId = 0;
        cekId = 0;
        cekVersion = 0;
        cekMdVersion = null;
        columnEncryptionKeyValues = new ArrayList<>();
    }

    int getSize() {
        return columnEncryptionKeyValues.size();
    }

    void add(byte[] encryptedKey,
            int dbId,
            int keyId,
            int keyVersion,
            byte[] mdVersion,
            String keyPath,
            String keyStoreName,
            String algorithmName) {

        assert null != columnEncryptionKeyValues : "columnEncryptionKeyValues should already be initialized.";

        if (aeLogger.isLoggable(java.util.logging.Level.FINE)) {
            aeLogger.fine("Retrieving CEK values");
        }

        EncryptionKeyInfo encryptionKey = new EncryptionKeyInfo(encryptedKey, dbId, keyId, keyVersion, mdVersion, keyPath, keyStoreName,
                algorithmName);
        columnEncryptionKeyValues.add(encryptionKey);

        if (0 == databaseId) {
            databaseId = dbId;
            cekId = keyId;
            cekVersion = keyVersion;
            cekMdVersion = mdVersion;
        }
        else {
            assert (databaseId == dbId);
            assert (cekId == keyId);
            assert (cekVersion == keyVersion);
            assert ((null != cekMdVersion) && (null != mdVersion) && (cekMdVersion.length == mdVersion.length));
        }
    }
}

/**
 * Contains all CEKs, each row represents one unique CEK (represented by CekTableEntry).
 */
class CekTable {
    CekTableEntry[] keyList;

    CekTable(int tableSize) {
        keyList = new CekTableEntry[tableSize];
    }

    int getSize() {
        return keyList.length;
    }

    CekTableEntry getCekTableEntry(int index) {
        return keyList[index];
    }

    void setCekTableEntry(int index,
            CekTableEntry entry) {
        keyList[index] = entry;
    }
}