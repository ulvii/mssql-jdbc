/*
 * Microsoft JDBC Driver for SQL Server
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * The top level TDS parser class.
 */
final class TDSParser {
    /** TDS protocol diagnostics logger */
    private static Logger logger = Logger.getLogger("com.microsoft.sqlserver.jdbc.internals.TDS.TOKEN");

    /*
     * Parses a TDS token stream from a reader using the supplied token handler. Parsing requires the ability to peek one byte ahead into the token
     * stream to determine the token type of the next token in the stream. When the token type has been determined, the token handler is called to
     * process the token (or not). Parsing continues until the token handler says to stop by returning false from one of the token handling methods.
     */
    static void parse(TDSReader tdsReader,
            String logContext) throws SQLServerException {
        parse(tdsReader, new TDSTokenHandler(logContext));
    }

    static void parse(TDSReader tdsReader,
            TDSTokenHandler tdsTokenHandler) throws SQLServerException {
        final boolean isLogging = logger.isLoggable(Level.FINEST);

        // Process TDS tokens from the token stream until we're told to stop.
        boolean parsing = true;

        // If TDS_LOGIN_ACK is received verify for TDS_FEATURE_EXTENSION_ACK packet
        boolean isLoginAck = false;
        boolean isFeatureExtAck = false;
        while (parsing) {
            int tdsTokenType = tdsReader.peekTokenType();
            if (isLogging) {
                logger.finest(tdsReader.toString() + ": " + tdsTokenHandler.logContext + ": Processing "
                        + ((-1 == tdsTokenType) ? "EOF" : TDS.getTokenName(tdsTokenType)));
            }

            switch (tdsTokenType) {
                case TDS.TDS_SSPI:
                    parsing = tdsTokenHandler.onSSPI(tdsReader);
                    break;
                case TDS.TDS_LOGIN_ACK:
                    isLoginAck = true;
                    parsing = tdsTokenHandler.onLoginAck(tdsReader);
                    break;
                case TDS.TDS_FEATURE_EXTENSION_ACK:
                    isFeatureExtAck = true;
                    tdsReader.getConnection().processFeatureExtAck(tdsReader);
                    parsing = true;
                    break;
                case TDS.TDS_ENV_CHG:
                    parsing = tdsTokenHandler.onEnvChange(tdsReader);
                    break;
                case TDS.TDS_RET_STAT:
                    parsing = tdsTokenHandler.onRetStatus(tdsReader);
                    break;
                case TDS.TDS_RETURN_VALUE:
                    parsing = tdsTokenHandler.onRetValue(tdsReader);
                    break;
                case TDS.TDS_DONEINPROC:
                case TDS.TDS_DONEPROC:
                case TDS.TDS_DONE:
                    tdsReader.getCommand().checkForInterrupt();
                    parsing = tdsTokenHandler.onDone(tdsReader);
                    break;

                case TDS.TDS_ERR:
                    parsing = tdsTokenHandler.onError(tdsReader);
                    break;
                case TDS.TDS_MSG:
                    parsing = tdsTokenHandler.onInfo(tdsReader);
                    break;
                case TDS.TDS_ORDER:
                    parsing = tdsTokenHandler.onOrder(tdsReader);
                    break;
                case TDS.TDS_COLMETADATA:
                    parsing = tdsTokenHandler.onColMetaData(tdsReader);
                    break;
                case TDS.TDS_ROW:
                    parsing = tdsTokenHandler.onRow(tdsReader);
                    break;
                case TDS.TDS_NBCROW:
                    parsing = tdsTokenHandler.onNBCRow(tdsReader);
                    break;
                case TDS.TDS_COLINFO:
                    parsing = tdsTokenHandler.onColInfo(tdsReader);
                    break;
                case TDS.TDS_TABNAME:
                    parsing = tdsTokenHandler.onTabName(tdsReader);
                    break;

                case TDS.TDS_FEDAUTHINFO:
                    parsing = tdsTokenHandler.onFedAuthInfo(tdsReader);
                    break;

                case -1:
                    tdsReader.getCommand().onTokenEOF();
                    tdsTokenHandler.onEOF(tdsReader);
                    parsing = false;
                    break;

                default:
                    throwUnexpectedTokenException(tdsReader, tdsTokenHandler.logContext);
                    break;
            }
        }

        // if TDS_FEATURE_EXTENSION_ACK is not received verify if TDS_FEATURE_EXT_AE was sent
        if (isLoginAck && !isFeatureExtAck)
            tdsReader.TryProcessFeatureExtAck(isFeatureExtAck);
    }

    /* Handle unexpected tokens - throw an exception */
    static void throwUnexpectedTokenException(TDSReader tdsReader,
            String logContext) throws SQLServerException {
        if (logger.isLoggable(Level.SEVERE))
            logger.severe(tdsReader.toString() + ": " + logContext + ": Encountered unexpected " + TDS.getTokenName(tdsReader.peekTokenType()));
        tdsReader.throwInvalidTDSToken(TDS.getTokenName(tdsReader.peekTokenType()));
    }

    /* Ignore a length-prefixed token */
    static void ignoreLengthPrefixedToken(TDSReader tdsReader) throws SQLServerException {
        tdsReader.readUnsignedByte(); // token type
        int envValueLength = tdsReader.readUnsignedShort();
        byte[] envValueData = new byte[envValueLength];
        tdsReader.readBytes(envValueData, 0, envValueLength);
    }
}

