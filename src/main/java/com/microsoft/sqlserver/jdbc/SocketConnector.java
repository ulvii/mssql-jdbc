/*
 * Microsoft JDBC Driver for SQL Server
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * This is used to connect a socket in a separate thread
 */
final class SocketConnector implements Runnable {
    // socket on which connection attempt would be made
    private final Socket socket;

    // the socketFinder associated with this connector
    private final SocketFinder socketFinder;

    // inetSocketAddress to connect to
    private final InetSocketAddress inetSocketAddress;

    // timeout in milliseconds
    private final int timeoutInMilliseconds;

    // Logging variables
    private static final Logger logger = Logger.getLogger("com.microsoft.sqlserver.jdbc.internals.SocketConnector");
    private final String traceID;

    // Id of the thread. used for diagnosis
    private final String threadID;

    // a counter used to give unique IDs to each connector thread.
    // this will have the id of the thread that was last created.
    private static long lastThreadID = 0;

    /**
     * Constructs a new SocketConnector object with the associated socket and socketFinder
     */
    SocketConnector(Socket socket,
            InetSocketAddress inetSocketAddress,
            int timeOutInMilliSeconds,
            SocketFinder socketFinder) {
        this.socket = socket;
        this.inetSocketAddress = inetSocketAddress;
        this.timeoutInMilliseconds = timeOutInMilliSeconds;
        this.socketFinder = socketFinder;
        this.threadID = Long.toString(nextThreadID());
        this.traceID = "SocketConnector:" + this.threadID + "(" + socketFinder.toString() + ")";
    }

    /**
     * If search for socket has not finished, this function tries to connect a socket(with a timeout) synchronously. It further notifies the
     * socketFinder the result of the connection attempt
     */
    public void run() {
        IOException exception = null;
        // Note that we do not need socketFinder lock here
        // as we update nothing in socketFinder based on the condition.
        // So, its perfectly fine to make a dirty read.
        SocketFinder.Result result = socketFinder.getResult();
        if (result.equals(SocketFinder.Result.UNKNOWN)) {
            try {
                if (logger.isLoggable(Level.FINER)) {
                    logger.finer(
                            this.toString() + " connecting to InetSocketAddress:" + inetSocketAddress + " with timeout:" + timeoutInMilliseconds);
                }

                socket.connect(inetSocketAddress, timeoutInMilliseconds);
            }
            catch (IOException ex) {
                if (logger.isLoggable(Level.FINER)) {
                    logger.finer(this.toString() + " exception:" + ex.getClass() + " with message:" + ex.getMessage()
                            + " occured while connecting to InetSocketAddress:" + inetSocketAddress);
                }
                exception = ex;
            }

            socketFinder.updateResult(socket, exception, this.toString());
        }

    }

    /**
     * Used for tracing
     * 
     * @return traceID string
     */
    public String toString() {
        return traceID;
    }

    /**
     * Generates the next unique thread id.
     */
    private static synchronized long nextThreadID() {
        if (lastThreadID == Long.MAX_VALUE) {
            if (logger.isLoggable(Level.FINER))
                logger.finer("Resetting the Id count");
            lastThreadID = 1;
        }
        else {
            lastThreadID++;
        }
        return lastThreadID;
    }
}