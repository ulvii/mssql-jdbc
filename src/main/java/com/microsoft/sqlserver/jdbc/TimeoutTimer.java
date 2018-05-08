/*
 * Microsoft JDBC Driver for SQL Server
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;

/**
 * Timer for use with Commands that support a timeout.
 *
 * Once started, the timer runs for the prescribed number of seconds unless stopped. If the timer runs out, it interrupts its associated Command with
 * a reason like "timed out".
 */
final class TimeoutTimer implements Runnable {
    private static final String threadGroupName = "mssql-jdbc-TimeoutTimer";
    private final int timeoutSeconds;
    private final TDSCommand command;
    private volatile Future<?> task;
    private final SQLServerConnection con;
    
    private static final ExecutorService executor = Executors.newCachedThreadPool(new ThreadFactory() {
        private final AtomicReference<ThreadGroup> tgr = new AtomicReference<>();
        private final AtomicInteger threadNumber = new AtomicInteger(0);

        @Override
        public Thread newThread(Runnable r)
        {
            ThreadGroup tg = tgr.get();

            if (tg == null || tg.isDestroyed())
            {
                tg = new ThreadGroup(threadGroupName);
                tgr.set(tg);
            }

            Thread t = new Thread(tg, r, tg.getName() + "-" + threadNumber.incrementAndGet());
            t.setDaemon(true);
            return t;
        }
    });

    private volatile boolean canceled = false;

    TimeoutTimer(int timeoutSeconds,
            TDSCommand command,
            SQLServerConnection con) {
        assert timeoutSeconds > 0;

        this.timeoutSeconds = timeoutSeconds;
        this.command = command;
        this.con = con;
    }

    final void start() {
        task = executor.submit(this);
    }

    final void stop() {
        task.cancel(true);
        canceled = true;
    }

    public void run() {
        int secondsRemaining = timeoutSeconds;
        try {
            // Poll every second while time is left on the timer.
            // Return if/when the timer is canceled.
            do {
                if (canceled)
                    return;

                Thread.sleep(1000);
            }
            while (--secondsRemaining > 0);
        }
        catch (InterruptedException e) {
            // re-interrupt the current thread, in order to restore the thread's interrupt status.
            Thread.currentThread().interrupt();
            return;
        }

        // If the timer wasn't canceled before it ran out of
        // time then interrupt the registered command.
        try {
            // If TCP Connection to server is silently dropped, exceeding the query timeout on the same connection does not throw SQLTimeoutException
            // The application stops responding instead until SocketTimeoutException is thrown. In this case, we must manually terminate the connection.
            if (null == command && null != con) {
                con.terminate(SQLServerException.DRIVER_ERROR_IO_FAILED, SQLServerException.getErrString("R_connectionIsClosed"));
            }
            else {
                // If the timer wasn't canceled before it ran out of
                // time then interrupt the registered command.
                command.interrupt(SQLServerException.getErrString("R_queryTimedOut"));
            }
        }
        catch (SQLServerException e) {
            // Unfortunately, there's nothing we can do if we
            // fail to time out the request. There is no way
            // to report back what happened.
            assert null != command;
            command.log(Level.FINE, "Command could not be timed out. Reason: " + e.getMessage());
        }
    }
}