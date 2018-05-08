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
 * A default TDS token handler with some meaningful default processing. Other token handlers should subclass from this one to override the defaults
 * and provide specialized functionality.
 *
 * ENVCHANGE_TOKEN Processes the ENVCHANGE
 *
 * RETURN_STATUS_TOKEN Ignores the returned value
 *
 * DONE_TOKEN DONEPROC_TOKEN DONEINPROC_TOKEN Ignores the returned value
 *
 * ERROR_TOKEN Remember the error and throw a SQLServerException with that error on EOF
 *
 * INFO_TOKEN ORDER_TOKEN COLINFO_TOKEN (not COLMETADATA_TOKEN) TABNAME_TOKEN Ignore the token
 *
 * EOF Throw a database exception with text from the last error token
 *
 * All other tokens Throw a TDS protocol error exception
 */
class TDSTokenHandler {
    final String logContext;

    private StreamError databaseError;

    /** TDS protocol diagnostics logger */
    private static Logger logger = Logger.getLogger("com.microsoft.sqlserver.jdbc.internals.TDS.TOKEN");

    final StreamError getDatabaseError() {
        return databaseError;
    }

    TDSTokenHandler(String logContext) {
        this.logContext = logContext;
    }

    boolean onSSPI(TDSReader tdsReader) throws SQLServerException {
        TDSParser.throwUnexpectedTokenException(tdsReader, logContext);
        return false;
    }

    boolean onLoginAck(TDSReader tdsReader) throws SQLServerException {
        TDSParser.throwUnexpectedTokenException(tdsReader, logContext);
        return false;
    }

    boolean onFeatureExtensionAck(TDSReader tdsReader) throws SQLServerException {
        TDSParser.throwUnexpectedTokenException(tdsReader, logContext);
        return false;
    }

    boolean onEnvChange(TDSReader tdsReader) throws SQLServerException {
        tdsReader.getConnection().processEnvChange(tdsReader);
        return true;
    }

    boolean onRetStatus(TDSReader tdsReader) throws SQLServerException {
        (new StreamRetStatus()).setFromTDS(tdsReader);
        return true;
    }

    boolean onRetValue(TDSReader tdsReader) throws SQLServerException {
        TDSParser.throwUnexpectedTokenException(tdsReader, logContext);
        return false;
    }

    boolean onDone(TDSReader tdsReader) throws SQLServerException {
        StreamDone doneToken = new StreamDone();
        doneToken.setFromTDS(tdsReader);
        return true;
    }

    boolean onError(TDSReader tdsReader) throws SQLServerException {
        if (null == databaseError) {
            databaseError = new StreamError();
            databaseError.setFromTDS(tdsReader);
        }
        else {
            (new StreamError()).setFromTDS(tdsReader);
        }

        return true;
    }

    boolean onInfo(TDSReader tdsReader) throws SQLServerException {
        TDSParser.ignoreLengthPrefixedToken(tdsReader);
        return true;
    }

    boolean onOrder(TDSReader tdsReader) throws SQLServerException {
        TDSParser.ignoreLengthPrefixedToken(tdsReader);
        return true;
    }

    boolean onColMetaData(TDSReader tdsReader) throws SQLServerException {
        //SHOWPLAN might be ON, instead of throwing an exception, ignore the column meta data
        if (logger.isLoggable(Level.SEVERE))
            logger.severe(tdsReader.toString() + ": " + logContext + ": Encountered "
                    + TDS.getTokenName(tdsReader.peekTokenType()) + ". SHOWPLAN is ON, ignoring.");
        return false;
    }

    boolean onRow(TDSReader tdsReader) throws SQLServerException {
        TDSParser.throwUnexpectedTokenException(tdsReader, logContext);
        return false;
    }

    boolean onNBCRow(TDSReader tdsReader) throws SQLServerException {
        TDSParser.throwUnexpectedTokenException(tdsReader, logContext);
        return false;
    }

    boolean onColInfo(TDSReader tdsReader) throws SQLServerException {
        TDSParser.ignoreLengthPrefixedToken(tdsReader);
        return true;
    }

    boolean onTabName(TDSReader tdsReader) throws SQLServerException {
        TDSParser.ignoreLengthPrefixedToken(tdsReader);
        return true;
    }

    void onEOF(TDSReader tdsReader) throws SQLServerException {
        if (null != getDatabaseError()) {
            SQLServerException.makeFromDatabaseError(tdsReader.getConnection(), null, getDatabaseError().getMessage(), getDatabaseError(), false);
        }
    }

    boolean onFedAuthInfo(TDSReader tdsReader) throws SQLServerException {
        tdsReader.getConnection().processFedAuthInfo(tdsReader, this);
        return true;
    }
}
