/*
 * Microsoft JDBC Driver for SQL Server
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

import java.sql.Timestamp;

final class TDS {
    // TDS protocol versions
    static final int VER_DENALI = 0x74000004; // TDS 7.4
    static final int VER_KATMAI = 0x730B0003; // TDS 7.3B(includes null bit compression)
    static final int VER_YUKON = 0x72090002; // TDS 7.2
    static final int VER_UNKNOWN = 0x00000000; // Unknown/uninitialized

    static final int TDS_RET_STAT = 0x79;
    static final int TDS_COLMETADATA = 0x81;
    static final int TDS_TABNAME = 0xA4;
    static final int TDS_COLINFO = 0xA5;
    static final int TDS_ORDER = 0xA9;
    static final int TDS_ERR = 0xAA;
    static final int TDS_MSG = 0xAB;
    static final int TDS_RETURN_VALUE = 0xAC;
    static final int TDS_LOGIN_ACK = 0xAD;
    static final int TDS_FEATURE_EXTENSION_ACK = 0xAE;
    static final int TDS_ROW = 0xD1;
    static final int TDS_NBCROW = 0xD2;
    static final int TDS_ENV_CHG = 0xE3;
    static final int TDS_SSPI = 0xED;
    static final int TDS_DONE = 0xFD;
    static final int TDS_DONEPROC = 0xFE;
    static final int TDS_DONEINPROC = 0xFF;
    static final int TDS_FEDAUTHINFO = 0xEE;

    // FedAuth
    static final int TDS_FEATURE_EXT_FEDAUTH = 0x02;
    static final int TDS_FEDAUTH_LIBRARY_SECURITYTOKEN = 0x01;
    static final int TDS_FEDAUTH_LIBRARY_ADAL = 0x02;
    static final int TDS_FEDAUTH_LIBRARY_RESERVED = 0x7F;
    static final byte ADALWORKFLOW_ACTIVEDIRECTORYPASSWORD = 0x01;
    static final byte ADALWORKFLOW_ACTIVEDIRECTORYINTEGRATED = 0x02;
    static final byte FEDAUTH_INFO_ID_STSURL = 0x01; // FedAuthInfoData is token endpoint URL from which to acquire fed auth token
    static final byte FEDAUTH_INFO_ID_SPN = 0x02; // FedAuthInfoData is the SPN to use for acquiring fed auth token

    // AE constants
    static final int TDS_FEATURE_EXT_AE = 0x04;
    static final int MAX_SUPPORTED_TCE_VERSION = 0x01; // max version
    static final int CUSTOM_CIPHER_ALGORITHM_ID = 0; // max version
    static final int AES_256_CBC = 1;
    static final int AEAD_AES_256_CBC_HMAC_SHA256 = 2;
    static final int AE_METADATA = 0x08;

    static final int TDS_TVP = 0xF3;
    static final int TVP_ROW = 0x01;
    static final int TVP_NULL_TOKEN = 0xFFFF;
    static final int TVP_STATUS_DEFAULT = 0x02;

    static final int TVP_ORDER_UNIQUE_TOKEN = 0x10;
    // TVP_ORDER_UNIQUE_TOKEN flags
    static final byte TVP_ORDERASC_FLAG = 0x1;
    static final byte TVP_ORDERDESC_FLAG = 0x2;
    static final byte TVP_UNIQUE_FLAG = 0x4;

    // TVP flags, may be used in other places
    static final int FLAG_NULLABLE = 0x01;
    static final int FLAG_TVP_DEFAULT_COLUMN = 0x200;

    static final int FEATURE_EXT_TERMINATOR = -1;
    
    // Sql_variant length
    static final int SQL_VARIANT_LENGTH = 8009;

    static final String getTokenName(int tdsTokenType) {
        switch (tdsTokenType) {
            case TDS_RET_STAT:
                return "TDS_RET_STAT (0x79)";
            case TDS_COLMETADATA:
                return "TDS_COLMETADATA (0x81)";
            case TDS_TABNAME:
                return "TDS_TABNAME (0xA4)";
            case TDS_COLINFO:
                return "TDS_COLINFO (0xA5)";
            case TDS_ORDER:
                return "TDS_ORDER (0xA9)";
            case TDS_ERR:
                return "TDS_ERR (0xAA)";
            case TDS_MSG:
                return "TDS_MSG (0xAB)";
            case TDS_RETURN_VALUE:
                return "TDS_RETURN_VALUE (0xAC)";
            case TDS_LOGIN_ACK:
                return "TDS_LOGIN_ACK (0xAD)";
            case TDS_FEATURE_EXTENSION_ACK:
                return "TDS_FEATURE_EXTENSION_ACK (0xAE)";
            case TDS_ROW:
                return "TDS_ROW (0xD1)";
            case TDS_NBCROW:
                return "TDS_NBCROW (0xD2)";
            case TDS_ENV_CHG:
                return "TDS_ENV_CHG (0xE3)";
            case TDS_SSPI:
                return "TDS_SSPI (0xED)";
            case TDS_DONE:
                return "TDS_DONE (0xFD)";
            case TDS_DONEPROC:
                return "TDS_DONEPROC (0xFE)";
            case TDS_DONEINPROC:
                return "TDS_DONEINPROC (0xFF)";
            case TDS_FEDAUTHINFO:
                return "TDS_FEDAUTHINFO (0xEE)";
            default:
                return "unknown token (0x" + Integer.toHexString(tdsTokenType).toUpperCase() + ")";
        }
    }

    // RPC ProcIDs for use with RPCRequest (PKT_RPC) calls
    static final short PROCID_SP_CURSOR = 1;
    static final short PROCID_SP_CURSOROPEN = 2;
    static final short PROCID_SP_CURSORPREPARE = 3;
    static final short PROCID_SP_CURSOREXECUTE = 4;
    static final short PROCID_SP_CURSORPREPEXEC = 5;
    static final short PROCID_SP_CURSORUNPREPARE = 6;
    static final short PROCID_SP_CURSORFETCH = 7;
    static final short PROCID_SP_CURSOROPTION = 8;
    static final short PROCID_SP_CURSORCLOSE = 9;
    static final short PROCID_SP_EXECUTESQL = 10;
    static final short PROCID_SP_PREPARE = 11;
    static final short PROCID_SP_EXECUTE = 12;
    static final short PROCID_SP_PREPEXEC = 13;
    static final short PROCID_SP_PREPEXECRPC = 14;
    static final short PROCID_SP_UNPREPARE = 15;

    // Constants for use with cursor RPCs
    static final short SP_CURSOR_OP_UPDATE = 1;
    static final short SP_CURSOR_OP_DELETE = 2;
    static final short SP_CURSOR_OP_INSERT = 4;
    static final short SP_CURSOR_OP_REFRESH = 8;
    static final short SP_CURSOR_OP_LOCK = 16;
    static final short SP_CURSOR_OP_SETPOSITION = 32;
    static final short SP_CURSOR_OP_ABSOLUTE = 64;

    // Constants for server-cursored result sets.
    // See the Engine Cursors Functional Specification for details.
    static final int FETCH_FIRST = 1;
    static final int FETCH_NEXT = 2;
    static final int FETCH_PREV = 4;
    static final int FETCH_LAST = 8;
    static final int FETCH_ABSOLUTE = 16;
    static final int FETCH_RELATIVE = 32;
    static final int FETCH_REFRESH = 128;
    static final int FETCH_INFO = 256;
    static final int FETCH_PREV_NOADJUST = 512;
    static final byte RPC_OPTION_NO_METADATA = (byte) 0x02;

    // Transaction manager request types
    static final short TM_GET_DTC_ADDRESS = 0;
    static final short TM_PROPAGATE_XACT = 1;
    static final short TM_BEGIN_XACT = 5;
    static final short TM_PROMOTE_PROMOTABLE_XACT = 6;
    static final short TM_COMMIT_XACT = 7;
    static final short TM_ROLLBACK_XACT = 8;
    static final short TM_SAVE_XACT = 9;

    static final byte PKT_QUERY = 1;
    static final byte PKT_RPC = 3;
    static final byte PKT_REPLY = 4;
    static final byte PKT_CANCEL_REQ = 6;
    static final byte PKT_BULK = 7;
    static final byte PKT_DTC = 14;
    static final byte PKT_LOGON70 = 16; // 0x10
    static final byte PKT_SSPI = 17;
    static final byte PKT_PRELOGIN = 18; // 0x12
    static final byte PKT_FEDAUTH_TOKEN_MESSAGE = 8;    // Authentication token for federated authentication

    static final byte STATUS_NORMAL = 0x00;
    static final byte STATUS_BIT_EOM = 0x01;
    static final byte STATUS_BIT_ATTENTION = 0x02;// this is called ignore bit in TDS spec
    static final byte STATUS_BIT_RESET_CONN = 0x08;

    // Various TDS packet size constants
    static final int INVALID_PACKET_SIZE = -1;
    static final int INITIAL_PACKET_SIZE = 4096;
    static final int MIN_PACKET_SIZE = 512;
    static final int MAX_PACKET_SIZE = 32767;
    static final int DEFAULT_PACKET_SIZE = 8000;
    static final int SERVER_PACKET_SIZE = 0; // Accept server's configured packet size

    // TDS packet header size and offsets
    static final int PACKET_HEADER_SIZE = 8;
    static final int PACKET_HEADER_MESSAGE_TYPE = 0;
    static final int PACKET_HEADER_MESSAGE_STATUS = 1;
    static final int PACKET_HEADER_MESSAGE_LENGTH = 2;
    static final int PACKET_HEADER_SPID = 4;
    static final int PACKET_HEADER_SEQUENCE_NUM = 6;
    static final int PACKET_HEADER_WINDOW = 7; // Reserved/Not used

    // MARS header length:
    // 2 byte header type
    // 8 byte transaction descriptor
    // 4 byte outstanding request count
    static final int MARS_HEADER_LENGTH = 18; // 2 byte header type, 8 byte transaction descriptor,
    static final int TRACE_HEADER_LENGTH = 26; // header length (4) + header type (2) + guid (16) + Sequence number size (4)

    static final short HEADERTYPE_TRACE = 3;  // trace header type

    // Message header length
    static final int MESSAGE_HEADER_LENGTH = MARS_HEADER_LENGTH + 4; // length includes message header itself

    static final byte B_PRELOGIN_OPTION_VERSION = 0x00;
    static final byte B_PRELOGIN_OPTION_ENCRYPTION = 0x01;
    static final byte B_PRELOGIN_OPTION_INSTOPT = 0x02;
    static final byte B_PRELOGIN_OPTION_THREADID = 0x03;
    static final byte B_PRELOGIN_OPTION_MARS = 0x04;
    static final byte B_PRELOGIN_OPTION_TRACEID = 0x05;
    static final byte B_PRELOGIN_OPTION_FEDAUTHREQUIRED = 0x06;
    static final byte B_PRELOGIN_OPTION_TERMINATOR = (byte) 0xFF;

    // Login option byte 1
    static final byte LOGIN_OPTION1_ORDER_X86 = 0x00;
    static final byte LOGIN_OPTION1_ORDER_6800 = 0x01;
    static final byte LOGIN_OPTION1_CHARSET_ASCII = 0x00;
    static final byte LOGIN_OPTION1_CHARSET_EBCDIC = 0x02;
    static final byte LOGIN_OPTION1_FLOAT_IEEE_754 = 0x00;
    static final byte LOGIN_OPTION1_FLOAT_VAX = 0x04;
    static final byte LOGIN_OPTION1_FLOAT_ND5000 = 0x08;
    static final byte LOGIN_OPTION1_DUMPLOAD_ON = 0x00;
    static final byte LOGIN_OPTION1_DUMPLOAD_OFF = 0x10;
    static final byte LOGIN_OPTION1_USE_DB_ON = 0x00;
    static final byte LOGIN_OPTION1_USE_DB_OFF = 0x20;
    static final byte LOGIN_OPTION1_INIT_DB_WARN = 0x00;
    static final byte LOGIN_OPTION1_INIT_DB_FATAL = 0x40;
    static final byte LOGIN_OPTION1_SET_LANG_OFF = 0x00;
    static final byte LOGIN_OPTION1_SET_LANG_ON = (byte) 0x80;

    // Login option byte 2
    static final byte LOGIN_OPTION2_INIT_LANG_WARN = 0x00;
    static final byte LOGIN_OPTION2_INIT_LANG_FATAL = 0x01;
    static final byte LOGIN_OPTION2_ODBC_OFF = 0x00;
    static final byte LOGIN_OPTION2_ODBC_ON = 0x02;
    static final byte LOGIN_OPTION2_TRAN_BOUNDARY_OFF = 0x00;
    static final byte LOGIN_OPTION2_TRAN_BOUNDARY_ON = 0x04;
    static final byte LOGIN_OPTION2_CACHE_CONNECTION_OFF = 0x00;
    static final byte LOGIN_OPTION2_CACHE_CONNECTION_ON = 0x08;
    static final byte LOGIN_OPTION2_USER_NORMAL = 0x00;
    static final byte LOGIN_OPTION2_USER_SERVER = 0x10;
    static final byte LOGIN_OPTION2_USER_REMUSER = 0x20;
    static final byte LOGIN_OPTION2_USER_SQLREPL = 0x30;
    static final byte LOGIN_OPTION2_INTEGRATED_SECURITY_OFF = 0x00;
    static final byte LOGIN_OPTION2_INTEGRATED_SECURITY_ON = (byte) 0x80;

    // Login option byte 3
    static final byte LOGIN_OPTION3_DEFAULT = 0x00;
    static final byte LOGIN_OPTION3_CHANGE_PASSWORD = 0x01;
    static final byte LOGIN_OPTION3_SEND_YUKON_BINARY_XML = 0x02;
    static final byte LOGIN_OPTION3_USER_INSTANCE = 0x04;
    static final byte LOGIN_OPTION3_UNKNOWN_COLLATION_HANDLING = 0x08;
    static final byte LOGIN_OPTION3_FEATURE_EXTENSION = 0x10;

    // Login type flag (bits 5 - 7 reserved for future use)
    static final byte LOGIN_SQLTYPE_DEFAULT = 0x00;
    static final byte LOGIN_SQLTYPE_TSQL = 0x01;
    static final byte LOGIN_SQLTYPE_ANSI_V1 = 0x02;
    static final byte LOGIN_SQLTYPE_ANSI89_L1 = 0x03;
    static final byte LOGIN_SQLTYPE_ANSI89_L2 = 0x04;
    static final byte LOGIN_SQLTYPE_ANSI89_IEF = 0x05;
    static final byte LOGIN_SQLTYPE_ANSI89_ENTRY = 0x06;
    static final byte LOGIN_SQLTYPE_ANSI89_TRANS = 0x07;
    static final byte LOGIN_SQLTYPE_ANSI89_INTER = 0x08;
    static final byte LOGIN_SQLTYPE_ANSI89_FULL = 0x09;

    static final byte LOGIN_OLEDB_OFF = 0x00;
    static final byte LOGIN_OLEDB_ON = 0x10;

    static final byte LOGIN_READ_ONLY_INTENT = 0x20;
    static final byte LOGIN_READ_WRITE_INTENT = 0x00;

    static final byte ENCRYPT_OFF = 0x00;
    static final byte ENCRYPT_ON = 0x01;
    static final byte ENCRYPT_NOT_SUP = 0x02;
    static final byte ENCRYPT_REQ = 0x03;
    static final byte ENCRYPT_INVALID = (byte) 0xFF;

    static final String getEncryptionLevel(int level) {
        switch (level) {
            case ENCRYPT_OFF:
                return "OFF";
            case ENCRYPT_ON:
                return "ON";
            case ENCRYPT_NOT_SUP:
                return "NOT SUPPORTED";
            case ENCRYPT_REQ:
                return "REQUIRED";
            default:
                return "unknown encryption level (0x" + Integer.toHexString(level).toUpperCase() + ")";
        }
    }

    // Prelogin packet length, including the tds header,
    // version, encrpytion, and traceid data sessions.
    // For detailed info, please check the definition of
    // preloginRequest in Prelogin function.
    static final byte B_PRELOGIN_MESSAGE_LENGTH = 67;
    static final byte B_PRELOGIN_MESSAGE_LENGTH_WITH_FEDAUTH = 73;

    // Scroll options and concurrency options lifted out
    // of the the Yukon cursors spec for sp_cursoropen.
    final static int SCROLLOPT_KEYSET = 1;
    final static int SCROLLOPT_DYNAMIC = 2;
    final static int SCROLLOPT_FORWARD_ONLY = 4;
    final static int SCROLLOPT_STATIC = 8;
    final static int SCROLLOPT_FAST_FORWARD = 16;

    final static int SCROLLOPT_PARAMETERIZED_STMT = 4096;
    final static int SCROLLOPT_AUTO_FETCH = 8192;
    final static int SCROLLOPT_AUTO_CLOSE = 16384;

    final static int CCOPT_READ_ONLY = 1;
    final static int CCOPT_SCROLL_LOCKS = 2;
    final static int CCOPT_OPTIMISTIC_CC = 4;
    final static int CCOPT_OPTIMISTIC_CCVAL = 8;
    final static int CCOPT_ALLOW_DIRECT = 8192;
    final static int CCOPT_UPDT_IN_PLACE = 16384;

    // Result set rows include an extra, "hidden" ROWSTAT column which indicates
    // the overall success or failure of the row fetch operation. With a keyset
    // cursor, the value in the ROWSTAT column indicates whether the row has been
    // deleted from the database.
    static final int ROWSTAT_FETCH_SUCCEEDED = 1;
    static final int ROWSTAT_FETCH_MISSING = 2;

    // ColumnInfo status
    final static int COLINFO_STATUS_EXPRESSION = 0x04;
    final static int COLINFO_STATUS_KEY = 0x08;
    final static int COLINFO_STATUS_HIDDEN = 0x10;
    final static int COLINFO_STATUS_DIFFERENT_NAME = 0x20;

    final static int MAX_FRACTIONAL_SECONDS_SCALE = 7;

    final static Timestamp MAX_TIMESTAMP = Timestamp.valueOf("2079-06-06 23:59:59");
    final static Timestamp MIN_TIMESTAMP = Timestamp.valueOf("1900-01-01 00:00:00");

    static int nanosSinceMidnightLength(int scale) {
        final int[] scaledLengths = {3, 3, 3, 4, 4, 5, 5, 5};
        assert scale >= 0;
        assert scale <= MAX_FRACTIONAL_SECONDS_SCALE;
        return scaledLengths[scale];
    }

    final static int DAYS_INTO_CE_LENGTH = 3;
    final static int MINUTES_OFFSET_LENGTH = 2;

    // Number of days in a "normal" (non-leap) year according to SQL Server.
    final static int DAYS_PER_YEAR = 365;

    final static int BASE_YEAR_1900 = 1900;
    final static int BASE_YEAR_1970 = 1970;
    final static String BASE_DATE_1970 = "1970-01-01";

    static int timeValueLength(int scale) {
        return nanosSinceMidnightLength(scale);
    }

    static int datetime2ValueLength(int scale) {
        return DAYS_INTO_CE_LENGTH + nanosSinceMidnightLength(scale);
    }

    static int datetimeoffsetValueLength(int scale) {
        return DAYS_INTO_CE_LENGTH + MINUTES_OFFSET_LENGTH + nanosSinceMidnightLength(scale);
    }

    // TDS is just a namespace - it can't be instantiated.
    private TDS() {
    }
}