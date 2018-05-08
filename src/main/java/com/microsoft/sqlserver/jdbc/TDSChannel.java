/*
 * Microsoft JDBC Driver for SQL Server
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.nio.channels.SocketChannel;
import java.security.KeyStore;
import java.security.Provider;
import java.security.Security;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.text.MessageFormat;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;

final class TDSChannel {
    private static final Logger logger = Logger.getLogger("com.microsoft.sqlserver.jdbc.internals.TDS.Channel");

    final Logger getLogger() {
        return logger;
    }

    private final String traceID;

    final public String toString() {
        return traceID;
    }

    private final SQLServerConnection con;

    private final TDSWriter tdsWriter;

    final TDSWriter getWriter() {
        return tdsWriter;
    }

    final TDSReader getReader(TDSCommand command) {
        return new TDSReader(this, con, command);
    }

    // Socket for raw TCP/IP communications with SQL Server
    private Socket tcpSocket;

    // Socket for SSL-encrypted communications with SQL Server
    private SSLSocket sslSocket;

    // Socket providing the communications interface to the driver.
    // For SSL-encrypted connections, this is the SSLSocket wrapped
    // around the TCP socket. For unencrypted connections, it is
    // just the TCP socket itself.
    private Socket channelSocket;

    // Implementation of a Socket proxy that can switch from TDS-wrapped I/O
    // (using the TDSChannel itself) during SSL handshake to raw I/O over
    // the TCP/IP socket.
    ProxySocket proxySocket = null;

    // I/O streams for raw TCP/IP communications with SQL Server
    private InputStream tcpInputStream;
    private OutputStream tcpOutputStream;

    // I/O streams providing the communications interface to the driver.
    // For SSL-encrypted connections, these are streams obtained from
    // the SSL socket above. They wrap the underlying TCP streams.
    // For unencrypted connections, they are just the TCP streams themselves.
    private InputStream inputStream;
    private OutputStream outputStream;

    /** TDS packet payload logger */
    private static Logger packetLogger = Logger.getLogger("com.microsoft.sqlserver.jdbc.internals.TDS.DATA");
    private final boolean isLoggingPackets = packetLogger.isLoggable(Level.FINEST);

    final boolean isLoggingPackets() {
        return isLoggingPackets;
    }

    // Number of TDS messages sent to and received from the server
    int numMsgsSent = 0;
    int numMsgsRcvd = 0;

    // Last SPID received from the server. Used for logging and to tag subsequent outgoing
    // packets to facilitate diagnosing problems from the server side.
    private int spid = 0;

    void setSPID(int spid) {
        this.spid = spid;
    }

    int getSPID() {
        return spid;
    }

    void resetPooledConnection() {
        tdsWriter.resetPooledConnection();
    }

    TDSChannel(SQLServerConnection con) {
        this.con = con;
        traceID = "TDSChannel (" + con.toString() + ")";
        this.tcpSocket = null;
        this.sslSocket = null;
        this.channelSocket = null;
        this.tcpInputStream = null;
        this.tcpOutputStream = null;
        this.inputStream = null;
        this.outputStream = null;
        this.tdsWriter = new TDSWriter(this, con);
    }

    /**
     * Opens the physical communications channel (TCP/IP socket and I/O streams) to the SQL Server.
     */
    final void open(String host,
            int port,
            int timeoutMillis,
            boolean useParallel,
            boolean useTnir,
            boolean isTnirFirstAttempt,
            int timeoutMillisForFullTimeout) throws SQLServerException {
        if (logger.isLoggable(Level.FINER))
            logger.finer(this.toString() + ": Opening TCP socket...");

        SocketFinder socketFinder = new SocketFinder(traceID, con);
        channelSocket = tcpSocket = socketFinder.findSocket(host, port, timeoutMillis, useParallel, useTnir, isTnirFirstAttempt,
                timeoutMillisForFullTimeout);

        try {

            // Set socket options
            tcpSocket.setTcpNoDelay(true);
            tcpSocket.setKeepAlive(true);

            // set SO_TIMEOUT
            int socketTimeout = con.getSocketTimeoutMilliseconds();
            tcpSocket.setSoTimeout(socketTimeout);

            inputStream = tcpInputStream = tcpSocket.getInputStream();
            outputStream = tcpOutputStream = tcpSocket.getOutputStream();
        }
        catch (IOException ex) {
            SQLServerException.ConvertConnectExceptionToSQLServerException(host, port, con, ex);
        }
    }

    /**
     * Disables SSL on this TDS channel.
     */
    void disableSSL() {
        if (logger.isLoggable(Level.FINER))
            logger.finer(toString() + " Disabling SSL...");

        /*
         * The mission: To close the SSLSocket and release everything that it is holding onto other than the TCP/IP socket and streams.
         *
         * The challenge: Simply closing the SSLSocket tries to do additional, unnecessary shutdown I/O over the TCP/IP streams that are bound to the
         * socket proxy, resulting in a not responding and confusing SQL Server.
         *
         * Solution: Rewire the ProxySocket's input and output streams (one more time) to closed streams. SSLSocket sees that the streams are already
         * closed and does not attempt to do any further I/O on them before closing itself.
         */

        // Create a couple of cheap closed streams
        InputStream is = new ByteArrayInputStream(new byte[0]);
        try {
            is.close();
        }
        catch (IOException e) {
            // No reason to expect a brand new ByteArrayInputStream not to close,
            // but just in case...
            logger.fine("Ignored error closing InputStream: " + e.getMessage());
        }

        OutputStream os = new ByteArrayOutputStream();
        try {
            os.close();
        }
        catch (IOException e) {
            // No reason to expect a brand new ByteArrayOutputStream not to close,
            // but just in case...
            logger.fine("Ignored error closing OutputStream: " + e.getMessage());
        }

        // Rewire the proxy socket to the closed streams
        if (logger.isLoggable(Level.FINEST))
            logger.finest(toString() + " Rewiring proxy streams for SSL socket close");
        proxySocket.setStreams(is, os);

        // Now close the SSL socket. It will see that the proxy socket's streams
        // are closed and not try to do any further I/O over them.
        try {
            if (logger.isLoggable(Level.FINER))
                logger.finer(toString() + " Closing SSL socket");

            sslSocket.close();
        }
        catch (IOException e) {
            // Don't care if we can't close the SSL socket. We're done with it anyway.
            logger.fine("Ignored error closing SSLSocket: " + e.getMessage());
        }

        // Do not close the proxy socket. Doing so would close our TCP socket
        // to which the proxy socket is bound. Instead, just null out the reference
        // to free up the few resources it holds onto.
        proxySocket = null;

        // Finally, with all of the SSL support out of the way, put the TDSChannel
        // back to using the TCP/IP socket and streams directly.
        inputStream = tcpInputStream;
        outputStream = tcpOutputStream;
        channelSocket = tcpSocket;
        sslSocket = null;

        if (logger.isLoggable(Level.FINER))
            logger.finer(toString() + " SSL disabled");
    }

    /**
     * Used during SSL handshake, this class implements an InputStream that reads SSL handshake response data (framed in TDS messages) from the TDS
     * channel.
     */
    private class SSLHandshakeInputStream extends InputStream {
        private final TDSReader tdsReader;
        private final SSLHandshakeOutputStream sslHandshakeOutputStream;

        private final Logger logger;
        private final String logContext;

        SSLHandshakeInputStream(TDSChannel tdsChannel,
                SSLHandshakeOutputStream sslHandshakeOutputStream) {
            this.tdsReader = tdsChannel.getReader(null);
            this.sslHandshakeOutputStream = sslHandshakeOutputStream;
            this.logger = tdsChannel.getLogger();
            this.logContext = tdsChannel.toString() + " (SSLHandshakeInputStream):";
        }

        /**
         * If there is no handshake response data available to be read from existing packets then this method ensures that the SSL handshake output
         * stream has been flushed to the server, and reads another packet (starting the next TDS response message).
         *
         * Note that simply using TDSReader.ensurePayload isn't sufficient as it does not automatically start the new response message.
         */
        private void ensureSSLPayload() throws IOException {
            if (0 == tdsReader.available()) {
                if (logger.isLoggable(Level.FINEST))
                    logger.finest(logContext + " No handshake response bytes available. Flushing SSL handshake output stream.");

                try {
                    sslHandshakeOutputStream.endMessage();
                }
                catch (SQLServerException e) {
                    logger.finer(logContext + " Ending TDS message threw exception:" + e.getMessage());
                    throw new IOException(e.getMessage());
                }

                if (logger.isLoggable(Level.FINEST))
                    logger.finest(logContext + " Reading first packet of SSL handshake response");

                try {
                    tdsReader.readPacket();
                }
                catch (SQLServerException e) {
                    logger.finer(logContext + " Reading response packet threw exception:" + e.getMessage());
                    throw new IOException(e.getMessage());
                }
            }
        }

        public long skip(long n) throws IOException {
            if (logger.isLoggable(Level.FINEST))
                logger.finest(logContext + " Skipping " + n + " bytes...");

            if (n <= 0)
                return 0;

            if (n > Integer.MAX_VALUE)
                n = Integer.MAX_VALUE;

            ensureSSLPayload();

            try {
                tdsReader.skip((int) n);
            }
            catch (SQLServerException e) {
                logger.finer(logContext + " Skipping bytes threw exception:" + e.getMessage());
                throw new IOException(e.getMessage());
            }

            return n;
        }

        private final byte oneByte[] = new byte[1];

        public int read() throws IOException {
            int bytesRead;

            while (0 == (bytesRead = readInternal(oneByte, 0, oneByte.length)))
                ;

            assert 1 == bytesRead || -1 == bytesRead;
            return 1 == bytesRead ? oneByte[0] : -1;
        }

        public int read(byte[] b) throws IOException {
            return readInternal(b, 0, b.length);
        }

        public int read(byte b[],
                int offset,
                int maxBytes) throws IOException {
            return readInternal(b, offset, maxBytes);
        }

        private int readInternal(byte b[],
                int offset,
                int maxBytes) throws IOException {
            if (logger.isLoggable(Level.FINEST))
                logger.finest(logContext + " Reading " + maxBytes + " bytes...");

            ensureSSLPayload();

            try {
                tdsReader.readBytes(b, offset, maxBytes);
            }
            catch (SQLServerException e) {
                logger.finer(logContext + " Reading bytes threw exception:" + e.getMessage());
                throw new IOException(e.getMessage());
            }

            return maxBytes;
        }
    }

    /**
     * Used during SSL handshake, this class implements an OutputStream that writes SSL handshake request data (framed in TDS messages) to the TDS
     * channel.
     */
    private class SSLHandshakeOutputStream extends OutputStream {
        private final TDSWriter tdsWriter;

        /** Flag indicating when it is necessary to start a new prelogin TDS message */
        private boolean messageStarted;

        private final Logger logger;
        private final String logContext;

        SSLHandshakeOutputStream(TDSChannel tdsChannel) {
            this.tdsWriter = tdsChannel.getWriter();
            this.messageStarted = false;
            this.logger = tdsChannel.getLogger();
            this.logContext = tdsChannel.toString() + " (SSLHandshakeOutputStream):";
        }

        public void flush() throws IOException {
            // It seems that the security provider implementation in some JVMs
            // (notably SunJSSE in the 6.0 JVM) likes to add spurious calls to
            // flush the SSL handshake output stream during SSL handshaking.
            // We need to ignore these calls because the SSL handshake payload
            // needs to be completely encapsulated in TDS. The SSL handshake
            // input stream always ensures that this output stream has been flushed
            // before trying to read the response.
            if (logger.isLoggable(Level.FINEST))
                logger.finest(logContext + " Ignored a request to flush the stream");
        }

        void endMessage() throws SQLServerException {
            // We should only be asked to end the message if we have started one
            assert messageStarted;

            if (logger.isLoggable(Level.FINEST))
                logger.finest(logContext + " Finishing TDS message");

            // Flush any remaining bytes through the writer. Since there may be fewer bytes
            // ready to send than a full TDS packet, we end the message here and start a new
            // one later if additional handshake data needs to be sent.
            tdsWriter.endMessage();
            messageStarted = false;
        }

        private final byte singleByte[] = new byte[1];

        public void write(int b) throws IOException {
            singleByte[0] = (byte) (b & 0xFF);
            writeInternal(singleByte, 0, singleByte.length);
        }

        public void write(byte[] b) throws IOException {
            writeInternal(b, 0, b.length);
        }

        public void write(byte[] b,
                int off,
                int len) throws IOException {
            writeInternal(b, off, len);
        }

        private void writeInternal(byte[] b,
                int off,
                int len) throws IOException {
            try {
                // Start out the handshake request in a new prelogin message. Subsequent
                // writes just add handshake data to the request until flushed.
                if (!messageStarted) {
                    if (logger.isLoggable(Level.FINEST))
                        logger.finest(logContext + " Starting new TDS packet...");

                    tdsWriter.startMessage(null, TDS.PKT_PRELOGIN);
                    messageStarted = true;
                }

                if (logger.isLoggable(Level.FINEST))
                    logger.finest(logContext + " Writing " + len + " bytes...");

                tdsWriter.writeBytes(b, off, len);
            }
            catch (SQLServerException e) {
                logger.finer(logContext + " Writing bytes threw exception:" + e.getMessage());
                throw new IOException(e.getMessage());
            }
        }
    }

    /**
     * This class implements an InputStream that just forwards all of its methods to an underlying InputStream.
     *
     * It is more predictable than FilteredInputStream which forwards some of its read methods directly to the underlying stream, but not others.
     */
    private final class ProxyInputStream extends InputStream {
        private InputStream filteredStream;

        ProxyInputStream(InputStream is) {
            filteredStream = is;
        }

        final void setFilteredStream(InputStream is) {
            filteredStream = is;
        }

        public long skip(long n) throws IOException {
            long bytesSkipped;

            if (logger.isLoggable(Level.FINEST))
                logger.finest(toString() + " Skipping " + n + " bytes");

            bytesSkipped = filteredStream.skip(n);

            if (logger.isLoggable(Level.FINEST))
                logger.finest(toString() + " Skipped " + n + " bytes");

            return bytesSkipped;
        }

        public int available() throws IOException {
            int bytesAvailable = filteredStream.available();

            if (logger.isLoggable(Level.FINEST))
                logger.finest(toString() + " " + bytesAvailable + " bytes available");

            return bytesAvailable;
        }

        private final byte oneByte[] = new byte[1];

        public int read() throws IOException {
            int bytesRead;

            while (0 == (bytesRead = readInternal(oneByte, 0, oneByte.length)))
                ;

            assert 1 == bytesRead || -1 == bytesRead;
            return 1 == bytesRead ? oneByte[0] : -1;
        }

        public int read(byte[] b) throws IOException {
            return readInternal(b, 0, b.length);
        }

        public int read(byte b[],
                int offset,
                int maxBytes) throws IOException {
            return readInternal(b, offset, maxBytes);
        }

        private int readInternal(byte b[],
                int offset,
                int maxBytes) throws IOException {
            int bytesRead;

            if (logger.isLoggable(Level.FINEST))
                logger.finest(toString() + " Reading " + maxBytes + " bytes");

            try {
                bytesRead = filteredStream.read(b, offset, maxBytes);
            }
            catch (IOException e) {
                if (logger.isLoggable(Level.FINER))
                    logger.finer(toString() + " " + e.getMessage());

                logger.finer(toString() + " Reading bytes threw exception:" + e.getMessage());
                throw e;
            }

            if (logger.isLoggable(Level.FINEST))
                logger.finest(toString() + " Read " + bytesRead + " bytes");

            return bytesRead;
        }

        public boolean markSupported() {
            boolean markSupported = filteredStream.markSupported();

            if (logger.isLoggable(Level.FINEST))
                logger.finest(toString() + " Returning markSupported: " + markSupported);

            return markSupported;
        }

        public void mark(int readLimit) {
            if (logger.isLoggable(Level.FINEST))
                logger.finest(toString() + " Marking next " + readLimit + " bytes");

            filteredStream.mark(readLimit);
        }

        public void reset() throws IOException {
            if (logger.isLoggable(Level.FINEST))
                logger.finest(toString() + " Resetting to previous mark");

            filteredStream.reset();
        }

        public void close() throws IOException {
            if (logger.isLoggable(Level.FINEST))
                logger.finest(toString() + " Closing");

            filteredStream.close();
        }
    }

    /**
     * This class implements an OutputStream that just forwards all of its methods to an underlying OutputStream.
     *
     * This class essentially does what FilteredOutputStream does, but is more efficient for our usage. FilteredOutputStream transforms block writes
     * to sequences of single-byte writes.
     */
    final class ProxyOutputStream extends OutputStream {
        private OutputStream filteredStream;

        ProxyOutputStream(OutputStream os) {
            filteredStream = os;
        }

        final void setFilteredStream(OutputStream os) {
            filteredStream = os;
        }

        public void close() throws IOException {
            if (logger.isLoggable(Level.FINEST))
                logger.finest(toString() + " Closing");

            filteredStream.close();
        }

        public void flush() throws IOException {
            if (logger.isLoggable(Level.FINEST))
                logger.finest(toString() + " Flushing");

            filteredStream.flush();
        }

        private final byte singleByte[] = new byte[1];

        public void write(int b) throws IOException {
            singleByte[0] = (byte) (b & 0xFF);
            writeInternal(singleByte, 0, singleByte.length);
        }

        public void write(byte[] b) throws IOException {
            writeInternal(b, 0, b.length);
        }

        public void write(byte[] b,
                int off,
                int len) throws IOException {
            writeInternal(b, off, len);
        }

        private void writeInternal(byte[] b,
                int off,
                int len) throws IOException {
            if (logger.isLoggable(Level.FINEST))
                logger.finest(toString() + " Writing " + len + " bytes");

            filteredStream.write(b, off, len);
        }
    }

    /**
     * This class implements a Socket whose I/O streams can be switched from using a TDSChannel for I/O to using its underlying TCP/IP socket.
     *
     * The SSL socket binds to a ProxySocket. The initial SSL handshake is done over TDSChannel I/O streams so that the handshake payload is framed in
     * TDS packets. The I/O streams are then switched to TCP/IP I/O streams using setStreams, and SSL communications continue directly over the TCP/IP
     * I/O streams.
     *
     * Most methods other than those for getting the I/O streams are simply forwarded to the TDSChannel's underlying TCP/IP socket. Methods that
     * change the socket binding or provide direct channel access are disallowed.
     */
    private class ProxySocket extends Socket {
        private final TDSChannel tdsChannel;
        private final Logger logger;
        private final String logContext;
        private final ProxyInputStream proxyInputStream;
        private final ProxyOutputStream proxyOutputStream;

        ProxySocket(TDSChannel tdsChannel) {
            this.tdsChannel = tdsChannel;
            this.logger = tdsChannel.getLogger();
            this.logContext = tdsChannel.toString() + " (ProxySocket):";

            // Create the I/O streams
            SSLHandshakeOutputStream sslHandshakeOutputStream = new SSLHandshakeOutputStream(tdsChannel);
            SSLHandshakeInputStream sslHandshakeInputStream = new SSLHandshakeInputStream(tdsChannel, sslHandshakeOutputStream);
            this.proxyOutputStream = new ProxyOutputStream(sslHandshakeOutputStream);
            this.proxyInputStream = new ProxyInputStream(sslHandshakeInputStream);
        }

        void setStreams(InputStream is,
                OutputStream os) {
            proxyInputStream.setFilteredStream(is);
            proxyOutputStream.setFilteredStream(os);
        }

        public InputStream getInputStream() throws IOException {
            if (logger.isLoggable(Level.FINEST))
                logger.finest(logContext + " Getting input stream");

            return proxyInputStream;
        }

        public OutputStream getOutputStream() throws IOException {
            if (logger.isLoggable(Level.FINEST))
                logger.finest(logContext + " Getting output stream");

            return proxyOutputStream;
        }

        // Allow methods that should just forward to the underlying TCP socket or return fixed values
        public InetAddress getInetAddress() {
            return tdsChannel.tcpSocket.getInetAddress();
        }

        public boolean getKeepAlive() throws SocketException {
            return tdsChannel.tcpSocket.getKeepAlive();
        }

        public InetAddress getLocalAddress() {
            return tdsChannel.tcpSocket.getLocalAddress();
        }

        public int getLocalPort() {
            return tdsChannel.tcpSocket.getLocalPort();
        }

        public SocketAddress getLocalSocketAddress() {
            return tdsChannel.tcpSocket.getLocalSocketAddress();
        }

        public boolean getOOBInline() throws SocketException {
            return tdsChannel.tcpSocket.getOOBInline();
        }

        public int getPort() {
            return tdsChannel.tcpSocket.getPort();
        }

        public int getReceiveBufferSize() throws SocketException {
            return tdsChannel.tcpSocket.getReceiveBufferSize();
        }

        public SocketAddress getRemoteSocketAddress() {
            return tdsChannel.tcpSocket.getRemoteSocketAddress();
        }

        public boolean getReuseAddress() throws SocketException {
            return tdsChannel.tcpSocket.getReuseAddress();
        }

        public int getSendBufferSize() throws SocketException {
            return tdsChannel.tcpSocket.getSendBufferSize();
        }

        public int getSoLinger() throws SocketException {
            return tdsChannel.tcpSocket.getSoLinger();
        }

        public int getSoTimeout() throws SocketException {
            return tdsChannel.tcpSocket.getSoTimeout();
        }

        public boolean getTcpNoDelay() throws SocketException {
            return tdsChannel.tcpSocket.getTcpNoDelay();
        }

        public int getTrafficClass() throws SocketException {
            return tdsChannel.tcpSocket.getTrafficClass();
        }

        public boolean isBound() {
            return true;
        }

        public boolean isClosed() {
            return false;
        }

        public boolean isConnected() {
            return true;
        }

        public boolean isInputShutdown() {
            return false;
        }

        public boolean isOutputShutdown() {
            return false;
        }

        public String toString() {
            return tdsChannel.tcpSocket.toString();
        }

        public SocketChannel getChannel() {
            return null;
        }

        // Disallow calls to methods that would change the underlying TCP socket
        public void bind(SocketAddress bindPoint) throws IOException {
            logger.finer(logContext + " Disallowed call to bind.  Throwing IOException.");
            throw new IOException();
        }

        public void connect(SocketAddress endpoint) throws IOException {
            logger.finer(logContext + " Disallowed call to connect (without timeout).  Throwing IOException.");
            throw new IOException();
        }

        public void connect(SocketAddress endpoint,
                int timeout) throws IOException {
            logger.finer(logContext + " Disallowed call to connect (with timeout).  Throwing IOException.");
            throw new IOException();
        }

        // Ignore calls to methods that would otherwise allow the SSL socket
        // to directly manipulate the underlying TCP socket
        public void close() throws IOException {
            if (logger.isLoggable(Level.FINER))
                logger.finer(logContext + " Ignoring close");
        }

        public void setReceiveBufferSize(int size) throws SocketException {
            if (logger.isLoggable(Level.FINER))
                logger.finer(toString() + " Ignoring setReceiveBufferSize size:" + size);
        }

        public void setSendBufferSize(int size) throws SocketException {
            if (logger.isLoggable(Level.FINER))
                logger.finer(toString() + " Ignoring setSendBufferSize size:" + size);
        }

        public void setReuseAddress(boolean on) throws SocketException {
            if (logger.isLoggable(Level.FINER))
                logger.finer(toString() + " Ignoring setReuseAddress");
        }

        public void setSoLinger(boolean on,
                int linger) throws SocketException {
            if (logger.isLoggable(Level.FINER))
                logger.finer(toString() + " Ignoring setSoLinger");
        }

        public void setSoTimeout(int timeout) throws SocketException {
            if (logger.isLoggable(Level.FINER))
                logger.finer(toString() + " Ignoring setSoTimeout");
        }

        public void setTcpNoDelay(boolean on) throws SocketException {
            if (logger.isLoggable(Level.FINER))
                logger.finer(toString() + " Ignoring setTcpNoDelay");
        }

        public void setTrafficClass(int tc) throws SocketException {
            if (logger.isLoggable(Level.FINER))
                logger.finer(toString() + " Ignoring setTrafficClass");
        }

        public void shutdownInput() throws IOException {
            if (logger.isLoggable(Level.FINER))
                logger.finer(toString() + " Ignoring shutdownInput");
        }

        public void shutdownOutput() throws IOException {
            if (logger.isLoggable(Level.FINER))
                logger.finer(toString() + " Ignoring shutdownOutput");
        }

        public void sendUrgentData(int data) throws IOException {
            if (logger.isLoggable(Level.FINER))
                logger.finer(toString() + " Ignoring sendUrgentData");
        }

        public void setKeepAlive(boolean on) throws SocketException {
            if (logger.isLoggable(Level.FINER))
                logger.finer(toString() + " Ignoring setKeepAlive");
        }

        public void setOOBInline(boolean on) throws SocketException {
            if (logger.isLoggable(Level.FINER))
                logger.finer(toString() + " Ignoring setOOBInline");
        }
    }

    /**
     * This class implements an X509TrustManager that always accepts the X509Certificate chain offered to it.
     *
     * A PermissiveX509TrustManager is used to "verify" the authenticity of the server when the trustServerCertificate connection property is set to
     * true.
     */
    private final class PermissiveX509TrustManager implements X509TrustManager {
        private final TDSChannel tdsChannel;
        private final Logger logger;
        private final String logContext;

        PermissiveX509TrustManager(TDSChannel tdsChannel) {
            this.tdsChannel = tdsChannel;
            this.logger = tdsChannel.getLogger();
            this.logContext = tdsChannel.toString() + " (PermissiveX509TrustManager):";
        }

        public void checkClientTrusted(X509Certificate[] chain,
                String authType) throws CertificateException {
            if (logger.isLoggable(Level.FINER))
                logger.finer(logContext + " Trusting client certificate (!)");
        }

        public void checkServerTrusted(X509Certificate[] chain,
                String authType) throws CertificateException {
            if (logger.isLoggable(Level.FINER))
                logger.finer(logContext + " Trusting server certificate");
        }

        public X509Certificate[] getAcceptedIssuers() {
            return new X509Certificate[0];
        }
    }

    /**
     * This class implements an X509TrustManager that hostname for validation.
     *
     * This validates the subject name in the certificate with the host name
     */
    private final class HostNameOverrideX509TrustManager implements X509TrustManager {
        private final Logger logger;
        private final String logContext;
        private final X509TrustManager defaultTrustManager;
        private String hostName;

        HostNameOverrideX509TrustManager(TDSChannel tdsChannel,
                X509TrustManager tm,
                String hostName) {
            this.logger = tdsChannel.getLogger();
            this.logContext = tdsChannel.toString() + " (HostNameOverrideX509TrustManager):";
            defaultTrustManager = tm;
            // canonical name is in lower case so convert this to lowercase too.
            this.hostName = hostName.toLowerCase(Locale.ENGLISH);
            ;
        }

        // Parse name in RFC 2253 format
        // Returns the common name if successful, null if failed to find the common name.
        // The parser tuned to be safe than sorry so if it sees something it cant parse correctly it returns null
        private String parseCommonName(String distinguishedName) {
            int index;
            // canonical name converts entire name to lowercase
            index = distinguishedName.indexOf("cn=");
            if (index == -1) {
                return null;
            }
            distinguishedName = distinguishedName.substring(index + 3);
            // Parse until a comma or end is reached
            // Note the parser will handle gracefully (essentially will return empty string) , inside the quotes (e.g cn="Foo, bar") however
            // RFC 952 says that the hostName cant have commas however the parser should not (and will not) crash if it sees a , within quotes.
            for (index = 0; index < distinguishedName.length(); index++) {
                if (distinguishedName.charAt(index) == ',') {
                    break;
                }
            }
            String commonName = distinguishedName.substring(0, index);
            // strip any quotes
            if (commonName.length() > 1 && ('\"' == commonName.charAt(0))) {
                if ('\"' == commonName.charAt(commonName.length() - 1))
                    commonName = commonName.substring(1, commonName.length() - 1);
                else {
                    // Be safe the name is not ended in " return null so the common Name wont match
                    commonName = null;
                }
            }
            return commonName;
        }

        private boolean validateServerName(String nameInCert) throws CertificateException {
            // Failed to get the common name from DN or empty CN
            if (null == nameInCert) {
                if (logger.isLoggable(Level.FINER))
                    logger.finer(logContext + " Failed to parse the name from the certificate or name is empty.");
                return false;
            }

            // Verify that the name in certificate matches exactly with the host name
            if (!nameInCert.equals(hostName)) {
                if (logger.isLoggable(Level.FINER))
                    logger.finer(logContext + " The name in certificate " + nameInCert + " does not match with the server name " + hostName + ".");
                return false;
            }

            if (logger.isLoggable(Level.FINER))
                logger.finer(logContext + " The name in certificate:" + nameInCert + " validated against server name " + hostName + ".");

            return true;
        }

        public void checkClientTrusted(X509Certificate[] chain,
                String authType) throws CertificateException {
            if (logger.isLoggable(Level.FINEST))
                logger.finest(logContext + " Forwarding ClientTrusted.");
            defaultTrustManager.checkClientTrusted(chain, authType);
        }

        public void checkServerTrusted(X509Certificate[] chain,
                String authType) throws CertificateException {
            if (logger.isLoggable(Level.FINEST))
                logger.finest(logContext + " Forwarding Trusting server certificate");
            defaultTrustManager.checkServerTrusted(chain, authType);
            if (logger.isLoggable(Level.FINEST))
                logger.finest(logContext + " default serverTrusted succeeded proceeding with server name validation");

            validateServerNameInCertificate(chain[0]);
        }

        private void validateServerNameInCertificate(X509Certificate cert) throws CertificateException {
            String nameInCertDN = cert.getSubjectX500Principal().getName("canonical");
            if (logger.isLoggable(Level.FINER)) {
                logger.finer(logContext + " Validating the server name:" + hostName);
                logger.finer(logContext + " The DN name in certificate:" + nameInCertDN);
            }

            boolean isServerNameValidated;

            // the name in cert is in RFC2253 format parse it to get the actual subject name
            String subjectCN = parseCommonName(nameInCertDN);

            isServerNameValidated = validateServerName(subjectCN);

            if (!isServerNameValidated) {

                Collection<List<?>> sanCollection = cert.getSubjectAlternativeNames();

                if (sanCollection != null) {
                    // find a subjectAlternateName entry corresponding to DNS Name
                    for (List<?> sanEntry : sanCollection) {

                        if (sanEntry != null && sanEntry.size() >= 2) {
                            Object key = sanEntry.get(0);
                            Object value = sanEntry.get(1);

                            if (logger.isLoggable(Level.FINER)) {
                                logger.finer(logContext + "Key: " + key + "; KeyClass:" + (key != null ? key.getClass() : null) + ";value: " + value
                                        + "; valueClass:" + (value != null ? value.getClass() : null));

                            }

                            // From Documentation(http://download.oracle.com/javase/6/docs/api/java/security/cert/X509Certificate.html):
                            // "Note that the Collection returned may contain
                            // more than one name of the same type."
                            // So, more than one entry of dnsNameType can be present.
                            // Java docs guarantee that the first entry in the list will be an integer.
                            // 2 is the sequence no of a dnsName
                            if ((key != null) && (key instanceof Integer) && ((Integer) key == 2)) {
                                // As per RFC2459, the DNSName will be in the
                                // "preferred name syntax" as specified by RFC
                                // 1034 and the name can be in upper or lower case.
                                // And no significance is attached to case.
                                // Java docs guarantee that the second entry in the list
                                // will be a string for dnsName
                                if (value != null && value instanceof String) {
                                    String dnsNameInSANCert = (String) value;

                                    // Use English locale to avoid Turkish i issues.
                                    // Note that, this conversion was not necessary for
                                    // cert.getSubjectX500Principal().getName("canonical");
                                    // as the above API already does this by default as per documentation.
                                    dnsNameInSANCert = dnsNameInSANCert.toLowerCase(Locale.ENGLISH);

                                    isServerNameValidated = validateServerName(dnsNameInSANCert);

                                    if (isServerNameValidated) {
                                        if (logger.isLoggable(Level.FINER)) {
                                            logger.finer(logContext + " found a valid name in certificate: " + dnsNameInSANCert);
                                        }
                                        break;
                                    }
                                }

                                if (logger.isLoggable(Level.FINER)) {
                                    logger.finer(logContext + " the following name in certificate does not match the serverName: " + value);
                                }
                            }

                        }
                        else {
                            if (logger.isLoggable(Level.FINER)) {
                                logger.finer(logContext + " found an invalid san entry: " + sanEntry);
                            }
                        }
                    }

                }
            }

            if (!isServerNameValidated) {
                String msg = SQLServerException.getErrString("R_certNameFailed");
                throw new CertificateException(msg);
            }
        }

        public X509Certificate[] getAcceptedIssuers() {
            return defaultTrustManager.getAcceptedIssuers();
        }
    }

    enum SSLHandhsakeState {
        SSL_HANDHSAKE_NOT_STARTED,
        SSL_HANDHSAKE_STARTED,
        SSL_HANDHSAKE_COMPLETE
    };

    /**
     * Enables SSL Handshake.
     * 
     * @param host
     *            Server Host Name for SSL Handshake
     * @param port
     *            Server Port for SSL Handshake
     * @throws SQLServerException
     */
    void enableSSL(String host,
            int port) throws SQLServerException {
        // If enabling SSL fails, which it can for a number of reasons, the following items
        // are used in logging information to the TDS channel logger to help diagnose the problem.
        Provider tmfProvider = null;        // TrustManagerFactory provider
        Provider sslContextProvider = null; // SSLContext provider
        Provider ksProvider = null;         // KeyStore provider
        String tmfDefaultAlgorithm = null;  // Default algorithm (typically X.509) used by the TrustManagerFactory
        SSLHandhsakeState handshakeState = SSLHandhsakeState.SSL_HANDHSAKE_NOT_STARTED;

        boolean isFips = false;
        String trustStoreType = null;
        String sslProtocol = null;

        // If anything in here fails, terminate the connection and throw an exception
        try {
            if (logger.isLoggable(Level.FINER))
                logger.finer(toString() + " Enabling SSL...");

            String trustStoreFileName = con.activeConnectionProperties.getProperty(SQLServerDriverStringProperty.TRUST_STORE.toString());
            String trustStorePassword = con.activeConnectionProperties.getProperty(SQLServerDriverStringProperty.TRUST_STORE_PASSWORD.toString());
            String hostNameInCertificate = con.activeConnectionProperties
                    .getProperty(SQLServerDriverStringProperty.HOSTNAME_IN_CERTIFICATE.toString());

            trustStoreType = con.activeConnectionProperties.getProperty(SQLServerDriverStringProperty.TRUST_STORE_TYPE.toString());
            
            if(StringUtils.isEmpty(trustStoreType)) {
                trustStoreType = SQLServerDriverStringProperty.TRUST_STORE_TYPE.getDefaultValue();
            }
            
            isFips = Boolean.valueOf(con.activeConnectionProperties.getProperty(SQLServerDriverBooleanProperty.FIPS.toString())); 
            sslProtocol = con.activeConnectionProperties.getProperty(SQLServerDriverStringProperty.SSL_PROTOCOL.toString());
            
            if (isFips) {
                validateFips(trustStoreType, trustStoreFileName);
            }

            assert TDS.ENCRYPT_OFF == con.getRequestedEncryptionLevel() || // Login only SSL
                    TDS.ENCRYPT_ON == con.getRequestedEncryptionLevel();   // Full SSL

            assert TDS.ENCRYPT_OFF == con.getNegotiatedEncryptionLevel() || // Login only SSL
                    TDS.ENCRYPT_ON == con.getNegotiatedEncryptionLevel() || // Full SSL
                    TDS.ENCRYPT_REQ == con.getNegotiatedEncryptionLevel();   // Full SSL

            // If we requested login only SSL or full SSL without server certificate validation,
            // then we'll "validate" the server certificate using a naive TrustManager that trusts
            // everything it sees.
            TrustManager[] tm = null;
            if (TDS.ENCRYPT_OFF == con.getRequestedEncryptionLevel()
                    || (TDS.ENCRYPT_ON == con.getRequestedEncryptionLevel() && con.trustServerCertificate())) {
                if (logger.isLoggable(Level.FINER))
                    logger.finer(toString() + " SSL handshake will trust any certificate");

                tm = new TrustManager[] {new PermissiveX509TrustManager(this)};
            }
            // Otherwise, we'll check if a specific TrustManager implemenation has been requested and
            // if so instantiate it, optionally specifying a constructor argument to customize it.
            else if (con.getTrustManagerClass() != null) {
                Class<?> tmClass = Class.forName(con.getTrustManagerClass());
                if (!TrustManager.class.isAssignableFrom(tmClass)) {
                    throw new IllegalArgumentException(
                            "The class specified by the trustManagerClass property must implement javax.net.ssl.TrustManager");
                }
                String constructorArg = con.getTrustManagerConstructorArg();
                if (constructorArg == null) {
                    tm = new TrustManager[] {(TrustManager) tmClass.getDeclaredConstructor().newInstance()};
                }
                else {
                    tm = new TrustManager[] {(TrustManager) tmClass.getDeclaredConstructor(String.class).newInstance(constructorArg)};
                }
            }
            // Otherwise, we'll validate the certificate using a real TrustManager obtained
            // from the a security provider that is capable of validating X.509 certificates.
            else {
                if (logger.isLoggable(Level.FINER))
                    logger.finer(toString() + " SSL handshake will validate server certificate");

                KeyStore ks = null;

                // If we are using the system default trustStore and trustStorePassword
                // then we can skip all of the KeyStore loading logic below.
                // The security provider's implementation takes care of everything for us.
                if (null == trustStoreFileName && null == trustStorePassword) {
                    if (logger.isLoggable(Level.FINER))
                        logger.finer(toString() + " Using system default trust store and password");
                }

                // Otherwise either the trustStore, trustStorePassword, or both was specified.
                // In that case, we need to load up a KeyStore ourselves.
                else {
                    // First, obtain an interface to a KeyStore that can load trust material
                    // stored in Java Key Store (JKS) format.
                    if (logger.isLoggable(Level.FINEST))
                        logger.finest(toString() + " Finding key store interface");


                    ks = KeyStore.getInstance(trustStoreType);
                    ksProvider = ks.getProvider();

                    // Next, load up the trust store file from the specified location.
                    // Note: This function returns a null InputStream if the trust store cannot
                    // be loaded. This is by design. See the method comment and documentation
                    // for KeyStore.load for details.
                    InputStream is = loadTrustStore(trustStoreFileName);

                    // Finally, load the KeyStore with the trust material (if any) from the
                    // InputStream and close the stream.
                    if (logger.isLoggable(Level.FINEST))
                        logger.finest(toString() + " Loading key store");

                    try {
                        ks.load(is, (null == trustStorePassword) ? null : trustStorePassword.toCharArray());
                    }
                    finally {
                        // We are done with the trustStorePassword (if set). Clear it for better security.
                        con.activeConnectionProperties.remove(SQLServerDriverStringProperty.TRUST_STORE_PASSWORD.toString());

                        // We are also done with the trust store input stream.
                        if (null != is) {
                            try {
                                is.close();
                            }
                            catch (IOException e) {
                                if (logger.isLoggable(Level.FINE))
                                    logger.fine(toString() + " Ignoring error closing trust material InputStream...");
                            }
                        }
                    }
                }

                // Either we now have a KeyStore populated with trust material or we are using the
                // default source of trust material (cacerts). Either way, we are now ready to
                // use a TrustManagerFactory to create a TrustManager that uses the trust material
                // to validate the server certificate.

                // Next step is to get a TrustManagerFactory that can produce TrustManagers
                // that understands X.509 certificates.
                TrustManagerFactory tmf = null;

                if (logger.isLoggable(Level.FINEST))
                    logger.finest(toString() + " Locating X.509 trust manager factory");

                tmfDefaultAlgorithm = TrustManagerFactory.getDefaultAlgorithm();
                tmf = TrustManagerFactory.getInstance(tmfDefaultAlgorithm);
                tmfProvider = tmf.getProvider();

                // Tell the TrustManagerFactory to give us TrustManagers that we can use to
                // validate the server certificate using the trust material in the KeyStore.
                if (logger.isLoggable(Level.FINEST))
                    logger.finest(toString() + " Getting trust manager");

                tmf.init(ks);
                tm = tmf.getTrustManagers();

                // if the host name in cert provided use it or use the host name Only if it is not FIPS
                if (!isFips) {
                    if (null != hostNameInCertificate) {
                        tm = new TrustManager[] {new HostNameOverrideX509TrustManager(this, (X509TrustManager) tm[0], hostNameInCertificate)};
                    }
                    else {
                        tm = new TrustManager[] {new HostNameOverrideX509TrustManager(this, (X509TrustManager) tm[0], host)};
                    }
                }
            } // end if (!con.trustServerCertificate())

            // Now, with a real or fake TrustManager in hand, get a context for creating a
            // SSL sockets through a SSL socket factory. We require at least TLS support.
            SSLContext sslContext = null;

            if (logger.isLoggable(Level.FINEST))
                logger.finest(toString() + " Getting TLS or better SSL context");

            sslContext = SSLContext.getInstance(sslProtocol);
            sslContextProvider = sslContext.getProvider();

            if (logger.isLoggable(Level.FINEST))
                logger.finest(toString() + " Initializing SSL context");

            sslContext.init(null, tm, null);

            // Got the SSL context. Now create an SSL socket over our own proxy socket
            // which we can toggle between TDS-encapsulated and raw communications.
            // Initially, the proxy is set to encapsulate the SSL handshake in TDS packets.
            proxySocket = new ProxySocket(this);

            if (logger.isLoggable(Level.FINEST))
                logger.finest(toString() + " Creating SSL socket");

            sslSocket = (SSLSocket) sslContext.getSocketFactory().createSocket(proxySocket, host, port, false); // don't close proxy when SSL socket
                                                                                                                // is closed     
            // At long last, start the SSL handshake ...
            if (logger.isLoggable(Level.FINER))
                logger.finer(toString() + " Starting SSL handshake");

            // TLS 1.2 intermittent exception happens here.
            handshakeState = SSLHandhsakeState.SSL_HANDHSAKE_STARTED;
            sslSocket.startHandshake();
            handshakeState = SSLHandhsakeState.SSL_HANDHSAKE_COMPLETE;

            // After SSL handshake is complete, rewire proxy socket to use raw TCP/IP streams ...
            if (logger.isLoggable(Level.FINEST))
                logger.finest(toString() + " Rewiring proxy streams after handshake");

            proxySocket.setStreams(inputStream, outputStream);

            // ... and rewire TDSChannel to use SSL streams.
            if (logger.isLoggable(Level.FINEST))
                logger.finest(toString() + " Getting SSL InputStream");

            inputStream = sslSocket.getInputStream();

            if (logger.isLoggable(Level.FINEST))
                logger.finest(toString() + " Getting SSL OutputStream");

            outputStream = sslSocket.getOutputStream();

            // SSL is now enabled; switch over the channel socket
            channelSocket = sslSocket;

            if (logger.isLoggable(Level.FINER))
                logger.finer(toString() + " SSL enabled");
        }
        catch (Exception e) {
            // Log the original exception and its source at FINER level
            if (logger.isLoggable(Level.FINER))
                logger.log(Level.FINER, e.getMessage(), e);

            // If enabling SSL fails, the following information may help diagnose the problem.
            // Do not use Level INFO or above which is sent to standard output/error streams.
            // This is because due to an intermittent TLS 1.2 connection issue, we will be retrying the connection and
            // do not want to print this message in console.
            if (logger.isLoggable(Level.FINER))
                logger.log(Level.FINER,
                        "java.security path: " + JAVA_SECURITY + "\n" + "Security providers: " + Arrays.asList(Security.getProviders()) + "\n"
                                + ((null != sslContextProvider) ? ("SSLContext provider info: " + sslContextProvider.getInfo() + "\n"
                                        + "SSLContext provider services:\n" + sslContextProvider.getServices() + "\n") : "")
                                + ((null != tmfProvider) ? ("TrustManagerFactory provider info: " + tmfProvider.getInfo() + "\n") : "")
                                + ((null != tmfDefaultAlgorithm) ? ("TrustManagerFactory default algorithm: " + tmfDefaultAlgorithm + "\n") : "")
                                + ((null != ksProvider) ? ("KeyStore provider info: " + ksProvider.getInfo() + "\n") : "") + "java.ext.dirs: "
                                + System.getProperty("java.ext.dirs"));

            MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_sslFailed"));
            Object[] msgArgs = {e.getMessage()};

            // It is important to get the localized message here, otherwise error messages won't match for different locales.
            String errMsg = e.getLocalizedMessage();
            // If the message is null replace it with the non-localized message or a dummy string. This can happen if a custom
            // TrustManager implementation is specified that does not provide localized messages.
            if (errMsg == null) {
                errMsg = e.getMessage();
            }
            if (errMsg == null) {
                errMsg = "";
            }
            // The error message may have a connection id appended to it. Extract the message only for comparison.
            // This client connection id is appended in method checkAndAppendClientConnId().
            if (errMsg.contains(SQLServerException.LOG_CLIENT_CONNECTION_ID_PREFIX)) {
                errMsg = errMsg.substring(0, errMsg.indexOf(SQLServerException.LOG_CLIENT_CONNECTION_ID_PREFIX));
            }

            // Isolate the TLS1.2 intermittent connection error.
            if (e instanceof IOException && (SSLHandhsakeState.SSL_HANDHSAKE_STARTED == handshakeState)
                    && (errMsg.equals(SQLServerException.getErrString("R_truncatedServerResponse")))) {
                con.terminate(SQLServerException.DRIVER_ERROR_INTERMITTENT_TLS_FAILED, form.format(msgArgs), e);
            }
            else {
                con.terminate(SQLServerException.DRIVER_ERROR_SSL_FAILED, form.format(msgArgs), e);
            }
        }
    }

    /**
     * Validate FIPS if fips set as true
     * 
     * Valid FIPS settings:
     * <LI>Encrypt should be true
     * <LI>trustServerCertificate should be false
     * <LI>if certificate is not installed TrustStoreType should be present.
     * 
     * @param trustStoreType
     * @param trustStoreFileName
     * @throws SQLServerException
     * @since 6.1.4
     */
    private void validateFips(final String trustStoreType,
            final String trustStoreFileName) throws SQLServerException {
        boolean isValid = false;
        boolean isEncryptOn;
        boolean isValidTrustStoreType;
        boolean isValidTrustStore;
        boolean isTrustServerCertificate;

        String strError = SQLServerException.getErrString("R_invalidFipsConfig");

        isEncryptOn = (TDS.ENCRYPT_ON == con.getRequestedEncryptionLevel());

        isValidTrustStoreType = !StringUtils.isEmpty(trustStoreType);
        isValidTrustStore = !StringUtils.isEmpty(trustStoreFileName);
        isTrustServerCertificate = con.trustServerCertificate();

        if (isEncryptOn && !isTrustServerCertificate) {          
            isValid = true;
            if (isValidTrustStore && !isValidTrustStoreType) {
            // In case of valid trust store we need to check TrustStoreType.
                isValid = false;               
                if (logger.isLoggable(Level.FINER))
                    logger.finer(toString() + "TrustStoreType is required alongside with TrustStore.");
            }
        }

        if (!isValid) {
            throw new SQLServerException(strError, null, 0, null);
        }

    }

    private final static String SEPARATOR = System.getProperty("file.separator");
    private final static String JAVA_HOME = System.getProperty("java.home");
    private final static String JAVA_SECURITY = JAVA_HOME + SEPARATOR + "lib" + SEPARATOR + "security";
    private final static String JSSECACERTS = JAVA_SECURITY + SEPARATOR + "jssecacerts";
    private final static String CACERTS = JAVA_SECURITY + SEPARATOR + "cacerts";

    /**
     * Loads the contents of a trust store into an InputStream.
     *
     * When a location to a trust store is specified, this method attempts to load that store. Otherwise, it looks for and attempts to load the
     * default trust store using essentially the same logic (outlined in the JSSE Reference Guide) as the default X.509 TrustManagerFactory.
     *
     * @return an InputStream containing the contents of the loaded trust store
     * @return null if the trust store cannot be loaded.
     *
     *         Note: It is by design that this function returns null when the trust store cannot be loaded rather than throwing an exception. The
     *         reason is that KeyStore.load, which uses the returned InputStream, interprets a null InputStream to mean that there are no trusted
     *         certificates, which mirrors the behavior of the default (no trust store, no password specified) path.
     */
    final InputStream loadTrustStore(String trustStoreFileName) {
        FileInputStream is = null;

        // First case: Trust store filename was specified
        if (null != trustStoreFileName) {
            try {
                if (logger.isLoggable(Level.FINEST))
                    logger.finest(toString() + " Opening specified trust store: " + trustStoreFileName);

                is = new FileInputStream(trustStoreFileName);
            }
            catch (FileNotFoundException e) {
                if (logger.isLoggable(Level.FINE))
                    logger.fine(toString() + " Trust store not found: " + e.getMessage());

                // If the trustStoreFileName connection property is set, but the file is not found,
                // then treat it as if the file was empty so that the TrustManager reports
                // that no certificate is found.
            }
        }

        // Second case: Trust store filename derived from javax.net.ssl.trustStore system property
        else if (null != (trustStoreFileName = System.getProperty("javax.net.ssl.trustStore"))) {
            try {
                if (logger.isLoggable(Level.FINEST))
                    logger.finest(toString() + " Opening default trust store (from javax.net.ssl.trustStore): " + trustStoreFileName);

                is = new FileInputStream(trustStoreFileName);
            }
            catch (FileNotFoundException e) {
                if (logger.isLoggable(Level.FINE))
                    logger.fine(toString() + " Trust store not found: " + e.getMessage());

                // If the javax.net.ssl.trustStore property is set, but the file is not found,
                // then treat it as if the file was empty so that the TrustManager reports
                // that no certificate is found.
            }
        }

        // Third case: No trust store specified and no system property set. Use jssecerts/cacerts.
        else {
            try {
                if (logger.isLoggable(Level.FINEST))
                    logger.finest(toString() + " Opening default trust store: " + JSSECACERTS);

                is = new FileInputStream(JSSECACERTS);
            }
            catch (FileNotFoundException e) {
                if (logger.isLoggable(Level.FINE))
                    logger.fine(toString() + " Trust store not found: " + e.getMessage());
            }

            // No jssecerts. Try again with cacerts...
            if (null == is) {
                try {
                    if (logger.isLoggable(Level.FINEST))
                        logger.finest(toString() + " Opening default trust store: " + CACERTS);

                    is = new FileInputStream(CACERTS);
                }
                catch (FileNotFoundException e) {
                    if (logger.isLoggable(Level.FINE))
                        logger.fine(toString() + " Trust store not found: " + e.getMessage());

                    // No jssecerts or cacerts. Treat it as if the trust store is empty so that
                    // the TrustManager reports that no certificate is found.
                }
            }
        }

        return is;
    }

    final int read(byte[] data,
            int offset,
            int length) throws SQLServerException {
        try {
            return inputStream.read(data, offset, length);
        }
        catch (IOException e) {
            if (logger.isLoggable(Level.FINE))
                logger.fine(toString() + " read failed:" + e.getMessage());

            if (e instanceof SocketTimeoutException) {
                con.terminate(SQLServerException.ERROR_SOCKET_TIMEOUT, e.getMessage(), e);
            }
            else {
                con.terminate(SQLServerException.DRIVER_ERROR_IO_FAILED, e.getMessage(), e);
            }

            return 0; // Keep the compiler happy.
        }
    }

    final void write(byte[] data,
            int offset,
            int length) throws SQLServerException {
        try {
            outputStream.write(data, offset, length);
        }
        catch (IOException e) {
            if (logger.isLoggable(Level.FINER))
                logger.finer(toString() + " write failed:" + e.getMessage());

            con.terminate(SQLServerException.DRIVER_ERROR_IO_FAILED, e.getMessage(), e);
        }
    }

    final void flush() throws SQLServerException {
        try {
            outputStream.flush();
        }
        catch (IOException e) {
            if (logger.isLoggable(Level.FINER))
                logger.finer(toString() + " flush failed:" + e.getMessage());

            con.terminate(SQLServerException.DRIVER_ERROR_IO_FAILED, e.getMessage(), e);
        }
    }

    final void close() {
        if (null != sslSocket)
            disableSSL();

        if (null != inputStream) {
            if (logger.isLoggable(Level.FINEST))
                logger.finest(this.toString() + ": Closing inputStream...");

            try {
                inputStream.close();
            }
            catch (IOException e) {
                if (logger.isLoggable(Level.FINE))
                    logger.log(Level.FINE, this.toString() + ": Ignored error closing inputStream", e);
            }
        }

        if (null != outputStream) {
            if (logger.isLoggable(Level.FINEST))
                logger.finest(this.toString() + ": Closing outputStream...");

            try {
                outputStream.close();
            }
            catch (IOException e) {
                if (logger.isLoggable(Level.FINE))
                    logger.log(Level.FINE, this.toString() + ": Ignored error closing outputStream", e);
            }
        }

        if (null != tcpSocket) {
            if (logger.isLoggable(Level.FINER))
                logger.finer(this.toString() + ": Closing TCP socket...");

            try {
                tcpSocket.close();
            }
            catch (IOException e) {
                if (logger.isLoggable(Level.FINE))
                    logger.log(Level.FINE, this.toString() + ": Ignored error closing socket", e);
            }
        }
    }

    /**
     * Logs TDS packet data to the com.microsoft.sqlserver.jdbc.TDS.DATA logger
     *
     * @param data
     *            the buffer containing the TDS packet payload data to log
     * @param nStartOffset
     *            offset into the above buffer from where to start logging
     * @param nLength
     *            length (in bytes) of payload
     * @param messageDetail
     *            other loggable details about the payload
     */
    /* L0 */ void logPacket(byte data[],
            int nStartOffset,
            int nLength,
            String messageDetail) {
        assert 0 <= nLength && nLength <= data.length;
        assert 0 <= nStartOffset && nStartOffset <= data.length;

        final char hexChars[] = {'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F'};

        final char printableChars[] = {'.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.',
                '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', ' ', '!', '\"', '#', '$', '%', '&', '\'', '(', ')', '*', '+', ',', '-', '.', '/',
                '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', ':', ';', '<', '=', '>', '?', '@', 'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J',
                'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z', '[', '\\', ']', '^', '_', '`', 'a', 'b', 'c', 'd',
                'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z', '{', '|', '}', '~', '.',
                '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.',
                '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.',
                '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.',
                '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.',
                '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.'};

        // Log message body lines have this form:
        //
        // "XX XX XX XX XX XX XX XX XX XX XX XX XX XX XX XX ................"
        // 012345678911111111112222222222333333333344444444445555555555666666
        // 01234567890123456789012345678901234567890123456789012345
        //
        final char lineTemplate[] = {' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ',
                ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ',

                ' ', ' ',

                '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.'};

        char logLine[] = new char[lineTemplate.length];
        System.arraycopy(lineTemplate, 0, logLine, 0, lineTemplate.length);

        // Logging builds up a string buffer for the entire log trace
        // before writing it out. So use an initial size large enough
        // that the buffer doesn't have to resize itself.
        StringBuilder logMsg = new StringBuilder(messageDetail.length() + // Message detail
                4 * nLength +            // 2-digit hex + space + ASCII, per byte
                4 * (1 + nLength / 16) +     // 2 extra spaces + CR/LF, per line (16 bytes per line)
                80);                   // Extra fluff: IP:Port, Connection #, SPID, ...

        // Format the headline like so:
        // /157.55.121.182:2983 Connection 1, SPID 53, Message info here ...
        //
        // Note: the log formatter itself timestamps what we write so we don't have
        // to do it again here.
        logMsg.append(tcpSocket.getLocalAddress().toString() + ":" + tcpSocket.getLocalPort() + " SPID:" + spid + " " + messageDetail + "\r\n");

        // Fill in the body of the log message, line by line, 16 bytes per line.
        int nBytesLogged = 0;
        int nBytesThisLine;
        while (true) {
            // Fill up the line with as many bytes as we can (up to 16 bytes)
            for (nBytesThisLine = 0; nBytesThisLine < 16 && nBytesLogged < nLength; nBytesThisLine++, nBytesLogged++) {
                int nUnsignedByteVal = (data[nStartOffset + nBytesLogged] + 256) % 256;
                logLine[3 * nBytesThisLine] = hexChars[nUnsignedByteVal / 16];
                logLine[3 * nBytesThisLine + 1] = hexChars[nUnsignedByteVal % 16];
                logLine[50 + nBytesThisLine] = printableChars[nUnsignedByteVal];
            }

            // Pad out the remainder with whitespace
            for (int nBytesJustified = nBytesThisLine; nBytesJustified < 16; nBytesJustified++) {
                logLine[3 * nBytesJustified] = ' ';
                logLine[3 * nBytesJustified + 1] = ' ';
            }

            logMsg.append(logLine, 0, 50 + nBytesThisLine);
            if (nBytesLogged == nLength)
                break;

            logMsg.append("\r\n");
        }

        if (packetLogger.isLoggable(Level.FINEST)) {
            packetLogger.finest(logMsg.toString());
        }
    }

    /**
     * Get the current socket SO_TIMEOUT value.
     *
     * @return the current socket timeout value
     * @throws IOException thrown if the socket timeout cannot be read
     */
    final int getNetworkTimeout() throws IOException {
        return tcpSocket.getSoTimeout();
    }

    /**
     * Set the socket SO_TIMEOUT value.
     *
     * @param timeout the socket timeout in milliseconds
     * @throws IOException thrown if the socket timeout cannot be set
     */
    final void setNetworkTimeout(int timeout) throws IOException {
        tcpSocket.setSoTimeout(timeout);
    }
}