/*
 * Microsoft JDBC Driver for SQL Server
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

/**
 * Represents Encryption related information of the cipher data.
 */
class CryptoMetadata
{
    TypeInfo baseTypeInfo;  
    CekTableEntry cekTableEntry;
    byte cipherAlgorithmId;
    String cipherAlgorithmName;
    SQLServerEncryptionType encryptionType;
    byte normalizationRuleVersion;
    SQLServerEncryptionAlgorithm cipherAlgorithm = null;
    EncryptionKeyInfo encryptionKeyInfo;
    short ordinal;

    CekTableEntry getCekTableEntry() {
        return cekTableEntry;
    }
    
    void setCekTableEntry(CekTableEntry cekTableEntryObj) {
        cekTableEntry = cekTableEntryObj;
    }
    
    TypeInfo getBaseTypeInfo() {
        return baseTypeInfo;
    }
    
    void setBaseTypeInfo(TypeInfo baseTypeInfoObj) {
        baseTypeInfo = baseTypeInfoObj;
    }
        
    SQLServerEncryptionAlgorithm getEncryptionAlgorithm() {
        return cipherAlgorithm;
    }
    
    void setEncryptionAlgorithm(SQLServerEncryptionAlgorithm encryptionAlgorithmObj) {
        cipherAlgorithm = encryptionAlgorithmObj;
    }
    
    EncryptionKeyInfo getEncryptionKeyInfo() {
        return encryptionKeyInfo;
    }
    
    void setEncryptionKeyInfo(EncryptionKeyInfo encryptionKeyInfoObj) {
        encryptionKeyInfo = encryptionKeyInfoObj;
    }

    byte getEncryptionAlgorithmId() {
        return cipherAlgorithmId;
    }

    String getEncryptionAlgorithmName() {
        return cipherAlgorithmName;
    }

    SQLServerEncryptionType getEncryptionType() {
        return encryptionType;
    }
    
    byte getNormalizationRuleVersion() {
        return normalizationRuleVersion;
    }

    short getOrdinal() {
        return ordinal;
    }
    
    CryptoMetadata(CekTableEntry cekTableEntryObj,
                                short ordinalVal,
                                byte cipherAlgorithmIdVal,
                                String cipherAlgorithmNameVal,
                                byte encryptionTypeVal,
                                byte normalizationRuleVersionVal) throws SQLServerException
   {
        cekTableEntry = cekTableEntryObj;
        ordinal = ordinalVal;
        cipherAlgorithmId = cipherAlgorithmIdVal;
        cipherAlgorithmName = cipherAlgorithmNameVal;
        encryptionType = SQLServerEncryptionType.of(encryptionTypeVal);
        normalizationRuleVersion = normalizationRuleVersionVal;
        encryptionKeyInfo = null;
    }

    boolean IsAlgorithmInitialized() {
        return null != cipherAlgorithm;
    }   
}