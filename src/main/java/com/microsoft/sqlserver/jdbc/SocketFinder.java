/*
 * Microsoft JDBC Driver for SQL Server
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

import java.io.IOException;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.text.MessageFormat;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Set;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * SocketFinder is used to find a server socket to which a connection can be made. This class abstracts the logic of finding a socket from TDSChannel
 * class.
 * 
 * In the case when useParallel is set to true, this is achieved by trying to make parallel connections to multiple IP addresses. This class is
 * responsible for spawning multiple threads and keeping track of the search result and the connected socket or exception to be thrown.
 * 
 * In the case where multiSubnetFailover is false, we try our old logic of trying to connect to the first ip address
 * 
 * Typical usage of this class is SocketFinder sf = new SocketFinder(traceId, conn); Socket = sf.getSocket(hostName, port, timeout);
 */
final class SocketFinder {
    /**
     * Indicates the result of a search
     */
    enum Result {
        UNKNOWN,// search is still in progress
        SUCCESS,// found a socket
        FAILURE// failed in finding a socket
    }

    // Thread pool - the values in the constructor are chosen based on the
    // explanation given in design_connection_director_multisubnet.doc
    private static final ThreadPoolExecutor threadPoolExecutor = new ThreadPoolExecutor(0, Integer.MAX_VALUE, 5, TimeUnit.SECONDS,
            new SynchronousQueue<Runnable>());

    // When parallel connections are to be used, use minimum timeout slice of 1500 milliseconds.
    private static final int minTimeoutForParallelConnections = 1500;

    // lock used for synchronization while updating
    // data within a socketFinder object
    private final Object socketFinderlock = new Object();

    // lock on which the parent thread would wait
    // after spawning threads.
    private final Object parentThreadLock = new Object();

    // indicates whether the socketFinder has succeeded or failed
    // in finding a socket or is still trying to find a socket
    private volatile Result result = Result.UNKNOWN;

    // total no of socket connector threads
    // spawned by a socketFinder object
    private int noOfSpawnedThreads = 0;

    // no of threads that finished their socket connection
    // attempts and notified socketFinder about their result
    private int noOfThreadsThatNotified = 0;

    // If a valid connected socket is found, this value would be non-null,
    // else this would be null
    private volatile Socket selectedSocket = null;

    // This would be one of the exceptions returned by the
    // socketConnector threads
    private volatile IOException selectedException = null;

    // Logging variables
    private static final Logger logger = Logger.getLogger("com.microsoft.sqlserver.jdbc.internals.SocketFinder");
    private final String traceID;

    // maximum number of IP Addresses supported
    private static final int ipAddressLimit = 64;

    // necessary for raising exceptions so that the connection pool can be notified
    private final SQLServerConnection conn;

    /**
     * Constructs a new SocketFinder object with appropriate traceId
     * 
     * @param callerTraceID
     *            traceID of the caller
     * @param sqlServerConnection
     *            the SQLServer connection
     */
    SocketFinder(String callerTraceID,
            SQLServerConnection sqlServerConnection) {
        traceID = "SocketFinder(" + callerTraceID + ")";
        conn = sqlServerConnection;
    }

    /**
     * Used to find a socket to which a connection can be made
     * 
     * @param hostName
     * @param portNumber
     * @param timeoutInMilliSeconds
     * @return connected socket
     * @throws IOException
     */
    Socket findSocket(String hostName,
            int portNumber,
            int timeoutInMilliSeconds,
            boolean useParallel,
            boolean useTnir,
            boolean isTnirFirstAttempt,
            int timeoutInMilliSecondsForFullTimeout) throws SQLServerException {
        assert timeoutInMilliSeconds != 0 : "The driver does not allow a time out of 0";

        try {
            InetAddress[] inetAddrs = null;

            // inetAddrs is only used if useParallel is true or TNIR is true. Skip resolving address if that's not the case.
            if (useParallel || useTnir) {
                // Ignore TNIR if host resolves to more than 64 IPs. Make sure we are using original timeout for this.
                inetAddrs = InetAddress.getAllByName(hostName);

                if ((useTnir) && (inetAddrs.length > ipAddressLimit)) {
                    useTnir = false;
                    timeoutInMilliSeconds = timeoutInMilliSecondsForFullTimeout;
                }
            }

            if (!useParallel) {
                // MSF is false. TNIR could be true or false. DBMirroring could be true or false.
                // For TNIR first attempt, we should do existing behavior including how host name is resolved.
                if (useTnir && isTnirFirstAttempt) {
                    return getDefaultSocket(hostName, portNumber, SQLServerConnection.TnirFirstAttemptTimeoutMs);
                }
                else if (!useTnir) {
                    return getDefaultSocket(hostName, portNumber, timeoutInMilliSeconds);
                }
            }

            // Code reaches here only if MSF = true or (TNIR = true and not TNIR first attempt)

            if (logger.isLoggable(Level.FINER)) {
                StringBuilder loggingString = new StringBuilder(this.toString());
                loggingString.append(" Total no of InetAddresses: ");
                loggingString.append(inetAddrs.length);
                loggingString.append(". They are: ");
                
                for (InetAddress inetAddr : inetAddrs) {
                    loggingString.append(inetAddr.toString() + ";");
                }

                logger.finer(loggingString.toString());
            }

            if (inetAddrs.length > ipAddressLimit) {
                MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_ipAddressLimitWithMultiSubnetFailover"));
                Object[] msgArgs = {Integer.toString(ipAddressLimit)};
                String errorStr = form.format(msgArgs);
                // we do not want any retry to happen here. So, terminate the connection
                // as the config is unsupported.
                conn.terminate(SQLServerException.DRIVER_ERROR_UNSUPPORTED_CONFIG, errorStr);
            }

            if (Util.isIBM()) {
                timeoutInMilliSeconds = Math.max(timeoutInMilliSeconds, minTimeoutForParallelConnections);
                if (logger.isLoggable(Level.FINER)) {
                    logger.finer(this.toString() + "Using Java NIO with timeout:" + timeoutInMilliSeconds);
                }
                findSocketUsingJavaNIO(inetAddrs, portNumber, timeoutInMilliSeconds);
            }
            else {
                LinkedList<InetAddress> inet4Addrs = new LinkedList<>();
                LinkedList<InetAddress> inet6Addrs = new LinkedList<>();

                for (InetAddress inetAddr : inetAddrs) {
                    if (inetAddr instanceof Inet4Address) {
                        inet4Addrs.add((Inet4Address) inetAddr);
                    }
                    else {
                        assert inetAddr instanceof Inet6Address : "Unexpected IP address " + inetAddr.toString();
                        inet6Addrs.add((Inet6Address) inetAddr);
                    }
                }

                // use half timeout only if both IPv4 and IPv6 addresses are present
                int timeoutForEachIPAddressType;
                if ((!inet4Addrs.isEmpty()) && (!inet6Addrs.isEmpty())) {
                    timeoutForEachIPAddressType = Math.max(timeoutInMilliSeconds / 2, minTimeoutForParallelConnections);
                }
                else
                    timeoutForEachIPAddressType = Math.max(timeoutInMilliSeconds, minTimeoutForParallelConnections);

                if (!inet4Addrs.isEmpty()) {
                    if (logger.isLoggable(Level.FINER)) {
                        logger.finer(this.toString() + "Using Java Threading with timeout:" + timeoutForEachIPAddressType);
                    }

                    findSocketUsingThreading(inet4Addrs, portNumber, timeoutForEachIPAddressType);
                }

                if (!result.equals(Result.SUCCESS)) {
                    // try threading logic
                    if (!inet6Addrs.isEmpty()) {
                        // do not start any threads if there is only one ipv6 address
                        if (inet6Addrs.size() == 1) {
                            return getConnectedSocket(inet6Addrs.get(0), portNumber, timeoutForEachIPAddressType);
                        }

                        if (logger.isLoggable(Level.FINER)) {
                            logger.finer(this.toString() + "Using Threading with timeout:" + timeoutForEachIPAddressType);
                        }

                        findSocketUsingThreading(inet6Addrs, portNumber, timeoutForEachIPAddressType);
                    }
                }
            }

            // If the thread continued execution due to timeout, the result may not be known.
            // In that case, update the result to failure. Note that this case is possible
            // for both IPv4 and IPv6.
            // Using double-checked locking for performance reasons.
            if (result.equals(Result.UNKNOWN)) {
                synchronized (socketFinderlock) {
                    if (result.equals(Result.UNKNOWN)) {
                        result = Result.FAILURE;
                        if (logger.isLoggable(Level.FINER)) {
                            logger.finer(this.toString() + " The parent thread updated the result to failure");
                        }
                    }
                }
            }

            // After we reach this point, there is no need for synchronization any more.
            // Because, the result would be known(success/failure).
            // And no threads would update SocketFinder
            // as their function calls would now be no-ops.
            if (result.equals(Result.FAILURE)) {
                if (selectedException == null) {
                    if (logger.isLoggable(Level.FINER)) {
                        logger.finer(this.toString()
                                + " There is no selectedException. The wait calls timed out before any connect call returned or timed out.");
                    }
                    String message = SQLServerException.getErrString("R_connectionTimedOut");
                    selectedException = new IOException(message);
                }
                throw selectedException;
            }

        }
        catch (InterruptedException ex) {
            // re-interrupt the current thread, in order to restore the thread's interrupt status.
            Thread.currentThread().interrupt();
            
            close(selectedSocket);
            SQLServerException.ConvertConnectExceptionToSQLServerException(hostName, portNumber, conn, ex);
        }
        catch (IOException ex) {
            close(selectedSocket);
            // The code below has been moved from connectHelper.
            // If we do not move it, the functions open(caller of findSocket)
            // and findSocket will have to
            // declare both IOException and SQLServerException in the throws clause
            // as we throw custom SQLServerExceptions(eg:IPAddressLimit, wrapping other exceptions
            // like interruptedException) in findSocket.
            // That would be a bit awkward, because connecthelper(the caller of open)
            // just wraps IOException into SQLServerException and throws SQLServerException.
            // Instead, it would be good to wrap all exceptions at one place - Right here, their origin.
            SQLServerException.ConvertConnectExceptionToSQLServerException(hostName, portNumber, conn, ex);

        }

        assert result.equals(Result.SUCCESS);
        assert selectedSocket != null : "Bug in code. Selected Socket cannot be null here.";

        return selectedSocket;
    }

    /**
     * This function uses java NIO to connect to all the addresses in inetAddrs with in a specified timeout. If it succeeds in connecting, it closes
     * all the other open sockets and updates the result to success.
     * 
     * @param inetAddrs
     *            the array of inetAddress to which connection should be made
     * @param portNumber
     *            the port number at which connection should be made
     * @param timeoutInMilliSeconds
     * @throws IOException
     */
    private void findSocketUsingJavaNIO(InetAddress[] inetAddrs,
            int portNumber,
            int timeoutInMilliSeconds) throws IOException {
        // The driver does not allow a time out of zero.
        // Also, the unit of time the user can specify in the driver is seconds.
        // So, even if the user specifies 1 second(least value), the least possible
        // value that can come here as timeoutInMilliSeconds is 500 milliseconds.
        assert timeoutInMilliSeconds != 0 : "The timeout cannot be zero";
        assert inetAddrs.length != 0 : "Number of inetAddresses should not be zero in this function";

        Selector selector = null;
        LinkedList<SocketChannel> socketChannels = new LinkedList<>();
        SocketChannel selectedChannel = null;

        try {
            selector = Selector.open();

            for (InetAddress inetAddr : inetAddrs) {
                SocketChannel sChannel = SocketChannel.open();
                socketChannels.add(sChannel);

                // make the channel non-blocking
                sChannel.configureBlocking(false);

                // register the channel for connect event
                int ops = SelectionKey.OP_CONNECT;
                SelectionKey key = sChannel.register(selector, ops);

                sChannel.connect(new InetSocketAddress(inetAddr, portNumber));

                if (logger.isLoggable(Level.FINER))
                    logger.finer(this.toString() + " initiated connection to address: " + inetAddr + ", portNumber: " + portNumber);
            }

            long timerNow = System.currentTimeMillis();
            long timerExpire = timerNow + timeoutInMilliSeconds;

            // Denotes the no of channels that still need to processed
            int noOfOutstandingChannels = inetAddrs.length;

            while (true) {
                long timeRemaining = timerExpire - timerNow;
                // if the timeout expired or a channel is selected or there are no more channels left to processes
                if ((timeRemaining <= 0) || (selectedChannel != null) || (noOfOutstandingChannels <= 0))
                    break;

                // denotes the no of channels that are ready to be processed. i.e. they are either connected
                // or encountered an exception while trying to connect
                int readyChannels = selector.select(timeRemaining);

                if (logger.isLoggable(Level.FINER))
                    logger.finer(this.toString() + " no of channels ready: " + readyChannels);

                // There are no real time guarantees on the time out of the select API used above.
                // This check is necessary
                // a) to guard against cases where the select returns faster than expected.
                // b) for cases where no channels could connect with in the time out
                if (readyChannels != 0) {
                    Set<SelectionKey> selectedKeys = selector.selectedKeys();
                    Iterator<SelectionKey> keyIterator = selectedKeys.iterator();

                    while (keyIterator.hasNext()) {

                        SelectionKey key = keyIterator.next();
                        SocketChannel ch = (SocketChannel) key.channel();

                        if (logger.isLoggable(Level.FINER))
                            logger.finer(this.toString() + " processing the channel :" + ch);// this traces the IP by default

                        boolean connected = false;
                        try {
                            connected = ch.finishConnect();

                            // ch.finishConnect should either return true or throw an exception
                            // as we have subscribed for OP_CONNECT.
                            assert connected == true : "finishConnect on channel:" + ch + " cannot be false";

                            selectedChannel = ch;

                            if (logger.isLoggable(Level.FINER))
                                logger.finer(this.toString() + " selected the channel :" + selectedChannel);

                            break;
                        }
                        catch (IOException ex) {
                            if (logger.isLoggable(Level.FINER))
                                logger.finer(this.toString() + " the exception: " + ex.getClass() + " with message: " + ex.getMessage()
                                        + " occured while processing the channel: " + ch);
                            updateSelectedException(ex, this.toString());
                            // close the channel pro-actively so that we do not
                            // rely to network resources
                            ch.close();
                        }

                        // unregister the key and remove from the selector's selectedKeys
                        key.cancel();
                        keyIterator.remove();
                        noOfOutstandingChannels--;
                    }
                }

                timerNow = System.currentTimeMillis();
            }
        }
        catch (IOException ex) {
            // in case of an exception, close the selected channel.
            // All other channels will be closed in the finally block,
            // as they need to be closed irrespective of a success/failure
            close(selectedChannel);
            throw ex;
        }
        finally {
            // close the selector
            // As per java docs, on selector.close(), any uncancelled keys still
            // associated with this
            // selector are invalidated, their channels are deregistered, and any other
            // resources associated with this selector are released.
            // So, its not necessary to cancel each key again
            close(selector);

            // Close all channels except the selected one.
            // As we close channels pro-actively in the try block,
            // its possible that we close a channel twice.
            // Closing a channel second time is a no-op.
            // This code is should be in the finally block to guard against cases where
            // we pre-maturely exit try block due to an exception in selector or other places.
            for (SocketChannel s : socketChannels) {
                if (s != selectedChannel) {
                    close(s);
                }
            }
        }

        // if a channel was selected, make the necessary updates
        if (selectedChannel != null) {
            // Note that this must be done after selector is closed. Otherwise,
            // we would get an illegalBlockingMode exception at run time.
            selectedChannel.configureBlocking(true);
            selectedSocket = selectedChannel.socket();

            result = Result.SUCCESS;
        }
    }

    // This method contains the old logic of connecting to
    // a socket of one of the IPs corresponding to a given host name.
    // In the old code below, the logic around 0 timeout has been removed as
    // 0 timeout is not allowed. The code has been re-factored so that the logic
    // is common for hostName or InetAddress.
    private Socket getDefaultSocket(String hostName,
            int portNumber,
            int timeoutInMilliSeconds) throws IOException {
        // Open the socket, with or without a timeout, throwing an UnknownHostException
        // if there is a failure to resolve the host name to an InetSocketAddress.
        //
        // Note that Socket(host, port) throws an UnknownHostException if the host name
        // cannot be resolved, but that InetSocketAddress(host, port) does not - it sets
        // the returned InetSocketAddress as unresolved.
        InetSocketAddress addr = new InetSocketAddress(hostName, portNumber);
        return getConnectedSocket(addr, timeoutInMilliSeconds);
    }

    private Socket getConnectedSocket(InetAddress inetAddr,
            int portNumber,
            int timeoutInMilliSeconds) throws IOException {
        InetSocketAddress addr = new InetSocketAddress(inetAddr, portNumber);
        return getConnectedSocket(addr, timeoutInMilliSeconds);
    }

    private Socket getConnectedSocket(InetSocketAddress addr,
            int timeoutInMilliSeconds) throws IOException {
        assert timeoutInMilliSeconds != 0 : "timeout cannot be zero";
        if (addr.isUnresolved())
            throw new java.net.UnknownHostException();
        selectedSocket = new Socket();
        selectedSocket.connect(addr, timeoutInMilliSeconds);
        return selectedSocket;
    }

    private void findSocketUsingThreading(LinkedList<InetAddress> inetAddrs,
            int portNumber,
            int timeoutInMilliSeconds) throws IOException, InterruptedException {
        assert timeoutInMilliSeconds != 0 : "The timeout cannot be zero";
        
        assert inetAddrs.isEmpty() == false : "Number of inetAddresses should not be zero in this function";

        LinkedList<Socket> sockets = new LinkedList<>();
        LinkedList<SocketConnector> socketConnectors = new LinkedList<>();

        try {

            // create a socket, inetSocketAddress and a corresponding socketConnector per inetAddress
            noOfSpawnedThreads = inetAddrs.size();
            for (InetAddress inetAddress : inetAddrs) {
                Socket s = new Socket();
                sockets.add(s);

                InetSocketAddress inetSocketAddress = new InetSocketAddress(inetAddress, portNumber);

                SocketConnector socketConnector = new SocketConnector(s, inetSocketAddress, timeoutInMilliSeconds, this);
                socketConnectors.add(socketConnector);
            }

            // acquire parent lock and spawn all threads
            synchronized (parentThreadLock) {
                for (SocketConnector sc : socketConnectors) {
                    threadPoolExecutor.execute(sc);
                }

                long timerNow = System.currentTimeMillis();
                long timerExpire = timerNow + timeoutInMilliSeconds;

                // The below loop is to guard against the spurious wake up problem
                while (true) {
                    long timeRemaining = timerExpire - timerNow;

                    if (logger.isLoggable(Level.FINER)) {
                        logger.finer(this.toString() + " TimeRemaining:" + timeRemaining + "; Result:" + result + "; Max. open thread count: "
                                + threadPoolExecutor.getLargestPoolSize() + "; Current open thread count:" + threadPoolExecutor.getActiveCount());
                    }

                    // if there is no time left or if the result is determined, break.
                    // Note that a dirty read of result is totally fine here.
                    // Since this thread holds the parentThreadLock, even if we do a dirty
                    // read here, the child thread, after updating the result, would not be
                    // able to call notify on the parentThreadLock
                    // (and thus finish execution) as it would be waiting on parentThreadLock
                    // held by this thread(the parent thread).
                    // So, this thread will wait again and then be notified by the childThread.
                    // On the other hand, if we try to take socketFinderLock here to avoid
                    // dirty read, we would introduce a dead lock due to the
                    // reverse order of locking in updateResult method.
                    if (timeRemaining <= 0 || (!result.equals(Result.UNKNOWN)))
                        break;

                    parentThreadLock.wait(timeRemaining);

                    if (logger.isLoggable(Level.FINER)) {
                        logger.finer(this.toString() + " The parent thread wokeup.");
                    }

                    timerNow = System.currentTimeMillis();
                }

            }

        }
        finally {
            // Close all sockets except the selected one.
            // As we close sockets pro-actively in the child threads,
            // its possible that we close a socket twice.
            // Closing a socket second time is a no-op.
            // If a child thread is waiting on the connect call on a socket s,
            // closing the socket s here ensures that an exception is thrown
            // in the child thread immediately. This mitigates the problem
            // of thread explosion by ensuring that unnecessary threads die
            // quickly without waiting for "min(timeOut, 21)" seconds
            for (Socket s : sockets) {
                if (s != selectedSocket) {
                    close(s);
                }
            }
        }
        
        if (selectedSocket != null) {          
            result = Result.SUCCESS;
        }
    }

    /**
     * search result
     */
    Result getResult() {
        return result;
    }

    void close(Selector selector) {
        if (null != selector) {
            if (logger.isLoggable(Level.FINER))
                logger.finer(this.toString() + ": Closing Selector");

            try {
                selector.close();
            }
            catch (IOException e) {
                if (logger.isLoggable(Level.FINE))
                    logger.log(Level.FINE, this.toString() + ": Ignored the following error while closing Selector", e);
            }
        }
    }

    void close(Socket socket) {
        if (null != socket) {
            if (logger.isLoggable(Level.FINER))
                logger.finer(this.toString() + ": Closing TCP socket:" + socket);

            try {
                socket.close();
            }
            catch (IOException e) {
                if (logger.isLoggable(Level.FINE))
                    logger.log(Level.FINE, this.toString() + ": Ignored the following error while closing socket", e);
            }
        }
    }

    void close(SocketChannel socketChannel) {
        if (null != socketChannel) {
            if (logger.isLoggable(Level.FINER))
                logger.finer(this.toString() + ": Closing TCP socket channel:" + socketChannel);

            try {
                socketChannel.close();
            }
            catch (IOException e) {
                if (logger.isLoggable(Level.FINE))
                    logger.log(Level.FINE, this.toString() + "Ignored the following error while closing socketChannel", e);
            }
        }
    }

    /**
     * Used by socketConnector threads to notify the socketFinder of their connection attempt result(a connected socket or exception). It updates the
     * result, socket and exception variables of socketFinder object. This method notifies the parent thread if a socket is found or if all the
     * spawned threads have notified. It also closes a socket if it is not selected for use by socketFinder.
     * 
     * @param socket
     *            the SocketConnector's socket
     * @param exception
     *            Exception that occurred in socket connector thread
     * @param threadId
     *            Id of the calling Thread for diagnosis
     */
    void updateResult(Socket socket,
            IOException exception,
            String threadId) {
        if (result.equals(Result.UNKNOWN)) {
            if (logger.isLoggable(Level.FINER)) {
                logger.finer("The following child thread is waiting for socketFinderLock:" + threadId);
            }

            synchronized (socketFinderlock) {
                if (logger.isLoggable(Level.FINER)) {
                    logger.finer("The following child thread acquired socketFinderLock:" + threadId);
                }

                if (result.equals(Result.UNKNOWN)) {
                    // if the connection was successful and no socket has been
                    // selected yet
                    if (exception == null && selectedSocket == null) {
                        selectedSocket = socket;
                        result = Result.SUCCESS;
                        if (logger.isLoggable(Level.FINER)) {
                            logger.finer("The socket of the following thread has been chosen:" + threadId);
                        }
                    }

                    // if an exception occurred
                    if (exception != null) {
                        updateSelectedException(exception, threadId);
                    }
                }

                noOfThreadsThatNotified++;

                // if all threads notified, but the result is still unknown,
                // update the result to failure
                if ((noOfThreadsThatNotified >= noOfSpawnedThreads) && result.equals(Result.UNKNOWN)) {
                    result = Result.FAILURE;
                }

                if (!result.equals(Result.UNKNOWN)) {
                    // 1) Note that at any point of time, there is only one
                    // thread(parent/child thread) competing for parentThreadLock.
                    // 2) The only time where a child thread could be waiting on
                    // parentThreadLock is before the wait call in the parentThread
                    // 3) After the above happens, the parent thread waits to be
                    // notified on parentThreadLock. After being notified,
                    // it would be the ONLY thread competing for the lock.
                    // for the following reasons
                    // a) The parentThreadLock is taken while holding the socketFinderLock.
                    // So, all child threads, except one, block on socketFinderLock
                    // (not parentThreadLock)
                    // b) After parentThreadLock is notified by a child thread, the result
                    // would be known(Refer the double-checked locking done at the
                    // start of this method). So, all child threads would exit
                    // as no-ops and would never compete with parent thread
                    // for acquiring parentThreadLock
                    // 4) As the parent thread is the only thread that competes for the
                    // parentThreadLock, it need not wait to acquire the lock once it wakes
                    // up and gets scheduled.
                    // This results in better performance as it would close unnecessary
                    // sockets and thus help child threads die quickly.

                    if (logger.isLoggable(Level.FINER)) {
                        logger.finer("The following child thread is waiting for parentThreadLock:" + threadId);
                    }

                    synchronized (parentThreadLock) {
                        if (logger.isLoggable(Level.FINER)) {
                            logger.finer("The following child thread acquired parentThreadLock:" + threadId);
                        }

                        parentThreadLock.notify();
                    }

                    if (logger.isLoggable(Level.FINER)) {
                        logger.finer("The following child thread released parentThreadLock and notified the parent thread:" + threadId);
                    }
                }
            }

            if (logger.isLoggable(Level.FINER)) {
                logger.finer("The following child thread released socketFinderLock:" + threadId);
            }
        }

    }

    /**
     * Updates the selectedException if
     * <p>
     * a) selectedException is null
     * <p>
     * b) ex is a non-socketTimeoutException and selectedException is a socketTimeoutException
     * <p>
     * If there are multiple exceptions, that are not related to socketTimeout the first non-socketTimeout exception is picked. If all exceptions are
     * related to socketTimeout, the first exception is picked. Note: This method is not thread safe. The caller should ensure thread safety.
     * 
     * @param ex
     *            the IOException
     * @param traceId
     *            the traceId of the thread
     */
    public void updateSelectedException(IOException ex,
            String traceId) {
        boolean updatedException = false;
        if (selectedException == null ||
            (!(ex instanceof SocketTimeoutException)) && (selectedException instanceof SocketTimeoutException)) {
            selectedException = ex;
            updatedException = true;
        }

        if (updatedException) {
            if (logger.isLoggable(Level.FINER)) {
                logger.finer("The selected exception is updated to the following: ExceptionType:" + ex.getClass() + "; ExceptionMessage:"
                        + ex.getMessage() + "; by the following thread:" + traceId);
            }
        }
    }

    /**
     * Used fof tracing
     * 
     * @return traceID string
     */
    public String toString() {
        return traceID;
    }
}