/*
 * Microsoft JDBC Driver for SQL Server
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

import java.io.ByteArrayInputStream;
import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.text.MessageFormat;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.Locale;
import java.util.SimpleTimeZone;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * TDSReader encapsulates the TDS response data stream.
 *
 * Bytes are read from SQL Server into a FIFO of packets. Reader methods traverse the packets to access the data.
 */
final class TDSReader {
    private final static Logger logger = Logger.getLogger("com.microsoft.sqlserver.jdbc.internals.TDS.Reader");
    final private String traceID;
    private TimeoutTimer tcpKeepAliveTimeoutTimer;
    
    final public String toString() {
        return traceID;
    }

    private final TDSChannel tdsChannel;
    private final SQLServerConnection con;

    private final TDSCommand command;

    final TDSCommand getCommand() {
        assert null != command;
        return command;
    }

    final SQLServerConnection getConnection() {
        return con;
    }

    private TDSPacket currentPacket = new TDSPacket(0);
    private TDSPacket lastPacket = currentPacket;
    private int payloadOffset = 0;
    private int packetNum = 0;

    private boolean isStreaming = true;
    private boolean useColumnEncryption = false;
    private boolean serverSupportsColumnEncryption = false;

    private final byte valueBytes[] = new byte[256];
    private static final AtomicInteger lastReaderID = new AtomicInteger(0);

    private static int nextReaderID() {
        return lastReaderID.incrementAndGet();
    }

    TDSReader(TDSChannel tdsChannel,
            SQLServerConnection con,
            TDSCommand command) {
        this.tdsChannel = tdsChannel;
        this.con = con;
        this.command = command; // may be null
        if(null != command) {
            //if cancelQueryTimeout is set, we should wait for the total amount of queryTimeout + cancelQueryTimeout to terminate the connection.
            this.tcpKeepAliveTimeoutTimer = (command.getCancelQueryTimeoutSeconds() > 0 && command.getQueryTimeoutSeconds() > 0 ) ? 
                (new TimeoutTimer(command.getCancelQueryTimeoutSeconds() + command.getQueryTimeoutSeconds(), null, con)) : null;
        }
        // if the logging level is not detailed than fine or more we will not have proper reader IDs.
        if (logger.isLoggable(Level.FINE))
            traceID = "TDSReader@" + nextReaderID() + " (" + con.toString() + ")";
        else
            traceID = con.toString();
        if (con.isColumnEncryptionSettingEnabled()) {
            useColumnEncryption = true;
        }
        serverSupportsColumnEncryption = con.getServerSupportsColumnEncryption();
    }

    final boolean isColumnEncryptionSettingEnabled() {
        return useColumnEncryption;
    }

    final boolean getServerSupportsColumnEncryption() {
        return serverSupportsColumnEncryption;
    }

    final void throwInvalidTDS() throws SQLServerException {
        if (logger.isLoggable(Level.SEVERE))
            logger.severe(toString() + " got unexpected value in TDS response at offset:" + payloadOffset);
        con.throwInvalidTDS();
    }

    final void throwInvalidTDSToken(String tokenName) throws SQLServerException {
        if (logger.isLoggable(Level.SEVERE))
            logger.severe(toString() + " got unexpected value in TDS response at offset:" + payloadOffset);
        con.throwInvalidTDSToken(tokenName);
    }

    /**
     * Ensures that payload data is available to be read, automatically advancing to (and possibly reading) the next packet.
     *
     * @return true if additional data is available to be read false if no more data is available
     */
    private boolean ensurePayload() throws SQLServerException {
        if (payloadOffset == currentPacket.payloadLength)
            if (!nextPacket())
                return false;
        assert payloadOffset < currentPacket.payloadLength;
        return true;
    }

    /**
     * Advance (and possibly read) the next packet.
     *
     * @return true if additional data is available to be read false if no more data is available
     */
    private boolean nextPacket() throws SQLServerException {
        assert null != currentPacket;

        // Shouldn't call this function unless we're at the end of the current packet...
        TDSPacket consumedPacket = currentPacket;
        assert payloadOffset == consumedPacket.payloadLength;

        // If no buffered packets are left then maybe we can read one...
        // This action must be synchronized against against another thread calling
        // readAllPackets() to read in ALL of the remaining packets of the current response.
        if (null == consumedPacket.next) {
            readPacket();

            if (null == consumedPacket.next)
                return false;
        }

        // Advance to that packet. If we are streaming through the
        // response, then unlink the current packet from the next
        // before moving to allow the packet to be reclaimed.
        TDSPacket nextPacket = consumedPacket.next;
        if (isStreaming) {
            if (logger.isLoggable(Level.FINEST))
                logger.finest(toString() + " Moving to next packet -- unlinking consumed packet");

            consumedPacket.next = null;
        }
        currentPacket = nextPacket;
        payloadOffset = 0;
        return true;
    }

    /**
     * Reads the next packet of the TDS channel.
     *
     * This method is synchronized to guard against simultaneously reading packets from one thread that is processing the response and another thread
     * that is trying to buffer it with TDSCommand.detach().
     */
    synchronized final boolean readPacket() throws SQLServerException {
        if (null != command && !command.readingResponse())
            return false;

        // Number of packets in should always be less than number of packets out.
        // If the server has been notified for an interrupt, it may be less by
        // more than one packet.
        assert tdsChannel.numMsgsRcvd < tdsChannel.numMsgsSent : "numMsgsRcvd:" + tdsChannel.numMsgsRcvd + " should be less than numMsgsSent:"
                + tdsChannel.numMsgsSent;

        TDSPacket newPacket = new TDSPacket(con.getTDSPacketSize());
        if (null != tcpKeepAliveTimeoutTimer) {
            if (logger.isLoggable(Level.FINEST)) {
                logger.finest(this.toString() + ": starting timer...");
            }
            tcpKeepAliveTimeoutTimer.start();
        }
        // First, read the packet header.
        for (int headerBytesRead = 0; headerBytesRead < TDS.PACKET_HEADER_SIZE;) {
            int bytesRead = tdsChannel.read(newPacket.header, headerBytesRead, TDS.PACKET_HEADER_SIZE - headerBytesRead);
            if (bytesRead < 0) {
                if (logger.isLoggable(Level.FINER))
                    logger.finer(toString() + " Premature EOS in response. packetNum:" + packetNum + " headerBytesRead:" + headerBytesRead);

                con.terminate(SQLServerException.DRIVER_ERROR_IO_FAILED, ((0 == packetNum && 0 == headerBytesRead)
                        ? SQLServerException.getErrString("R_noServerResponse") : SQLServerException.getErrString("R_truncatedServerResponse")));
            }

            headerBytesRead += bytesRead;
        }
        
        // if execution was subject to timeout then stop timing
        if (null != tcpKeepAliveTimeoutTimer) {
            if (logger.isLoggable(Level.FINEST)) {
                logger.finest(this.toString() + ":stopping timer...");
            }
            tcpKeepAliveTimeoutTimer.stop();
        }
        // Header size is a 2 byte unsigned short integer in big-endian order.
        int packetLength = Util.readUnsignedShortBigEndian(newPacket.header, TDS.PACKET_HEADER_MESSAGE_LENGTH);

        // Make header size is properly bounded and compute length of the packet payload.
        if (packetLength < TDS.PACKET_HEADER_SIZE || packetLength > con.getTDSPacketSize()) {
            if (logger.isLoggable(Level.WARNING)) {
                logger.warning(
                        toString() + " TDS header contained invalid packet length:" + packetLength + "; packet size:" + con.getTDSPacketSize());
            }
            throwInvalidTDS();
        }

        newPacket.payloadLength = packetLength - TDS.PACKET_HEADER_SIZE;

        // Just grab the SPID for logging (another big-endian unsigned short).
        tdsChannel.setSPID(Util.readUnsignedShortBigEndian(newPacket.header, TDS.PACKET_HEADER_SPID));

        // Packet header looks good enough.
        // When logging, copy the packet header to the log buffer.
        byte[] logBuffer = null;
        if (tdsChannel.isLoggingPackets()) {
            logBuffer = new byte[packetLength];
            System.arraycopy(newPacket.header, 0, logBuffer, 0, TDS.PACKET_HEADER_SIZE);
        }

        // Now for the payload...
        for (int payloadBytesRead = 0; payloadBytesRead < newPacket.payloadLength;) {
            int bytesRead = tdsChannel.read(newPacket.payload, payloadBytesRead, newPacket.payloadLength - payloadBytesRead);
            if (bytesRead < 0)
                con.terminate(SQLServerException.DRIVER_ERROR_IO_FAILED, SQLServerException.getErrString("R_truncatedServerResponse"));

            payloadBytesRead += bytesRead;
        }

        ++packetNum;

        lastPacket.next = newPacket;
        lastPacket = newPacket;

        // When logging, append the payload to the log buffer and write out the whole thing.
        if (tdsChannel.isLoggingPackets()) {
            System.arraycopy(newPacket.payload, 0, logBuffer, TDS.PACKET_HEADER_SIZE, newPacket.payloadLength);
            tdsChannel.logPacket(logBuffer, 0, packetLength,
                    this.toString() + " received Packet:" + packetNum + " (" + newPacket.payloadLength + " bytes)");
        }

        // If end of message, then bump the count of messages received and disable
        // interrupts. If an interrupt happened prior to disabling, then expect
        // to read the attention ack packet as well.
        if (newPacket.isEOM()) {
            ++tdsChannel.numMsgsRcvd;

            // Notify the command (if any) that we've reached the end of the response.
            if (null != command)
                command.onResponseEOM();
        }

        return true;
    }

    final TDSReaderMark mark() {
        TDSReaderMark mark = new TDSReaderMark(currentPacket, payloadOffset);
        isStreaming = false;

        if (logger.isLoggable(Level.FINEST))
            logger.finest(this.toString() + ": Buffering from: " + mark.toString());

        return mark;
    }

    final void reset(TDSReaderMark mark) {
        if (logger.isLoggable(Level.FINEST))
            logger.finest(this.toString() + ": Resetting to: " + mark.toString());

        currentPacket = mark.packet;
        payloadOffset = mark.payloadOffset;
    }

    final void stream() {
        isStreaming = true;
    }

    /**
     * Returns the number of bytes that can be read (or skipped over) from this TDSReader without blocking by the next caller of a method for this
     * TDSReader.
     *
     * @return the actual number of bytes available.
     */
    final int available() {
        // The number of bytes that can be read without blocking is just the number
        // of bytes that are currently buffered. That is the number of bytes left
        // in the current packet plus the number of bytes in the remaining packets.
        int available = currentPacket.payloadLength - payloadOffset;
        for (TDSPacket packet = currentPacket.next; null != packet; packet = packet.next)
            available += packet.payloadLength;
        return available;
    }

    /**
     * 
     * @return number of bytes available in the current packet
     */
    final int availableCurrentPacket() {
        /*
         * The number of bytes that can be read from the current chunk, without including the next chunk that is buffered. This is so the driver can
         * confirm if the next chunk sent is new packet or just continuation
         */
        int available = currentPacket.payloadLength - payloadOffset;
        return available;
    }

    final int peekTokenType() throws SQLServerException {
        // Check whether we're at EOF
        if (!ensurePayload())
            return -1;

        // Peek at the current byte (don't increment payloadOffset!)
        return currentPacket.payload[payloadOffset] & 0xFF;
    }

    final short peekStatusFlag() throws SQLServerException {
        // skip the current packet(i.e, TDS packet type) and peek into the status flag (USHORT)
        if (payloadOffset + 3 <= currentPacket.payloadLength) {
            short value = Util.readShort(currentPacket.payload, payloadOffset + 1);
            return value;
        }

        return 0;
    }

    final int readUnsignedByte() throws SQLServerException {
        // Ensure that we have a packet to read from.
        if (!ensurePayload())
            throwInvalidTDS();

        return currentPacket.payload[payloadOffset++] & 0xFF;
    }

    final short readShort() throws SQLServerException {
        if (payloadOffset + 2 <= currentPacket.payloadLength) {
            short value = Util.readShort(currentPacket.payload, payloadOffset);
            payloadOffset += 2;
            return value;
        }

        return Util.readShort(readWrappedBytes(2), 0);
    }

    final int readUnsignedShort() throws SQLServerException {
        if (payloadOffset + 2 <= currentPacket.payloadLength) {
            int value = Util.readUnsignedShort(currentPacket.payload, payloadOffset);
            payloadOffset += 2;
            return value;
        }

        return Util.readUnsignedShort(readWrappedBytes(2), 0);
    }

    final String readUnicodeString(int length) throws SQLServerException {
        int byteLength = 2 * length;
        byte bytes[] = new byte[byteLength];
        readBytes(bytes, 0, byteLength);
        return Util.readUnicodeString(bytes, 0, byteLength, con);

    }

    final char readChar() throws SQLServerException {
        return (char) readShort();
    }

    final int readInt() throws SQLServerException {
        if (payloadOffset + 4 <= currentPacket.payloadLength) {
            int value = Util.readInt(currentPacket.payload, payloadOffset);
            payloadOffset += 4;
            return value;
        }

        return Util.readInt(readWrappedBytes(4), 0);
    }

    final int readIntBigEndian() throws SQLServerException {
        if (payloadOffset + 4 <= currentPacket.payloadLength) {
            int value = Util.readIntBigEndian(currentPacket.payload, payloadOffset);
            payloadOffset += 4;
            return value;
        }

        return Util.readIntBigEndian(readWrappedBytes(4), 0);
    }

    final long readUnsignedInt() throws SQLServerException {
        return readInt() & 0xFFFFFFFFL;
    }

    final long readLong() throws SQLServerException {
        if (payloadOffset + 8 <= currentPacket.payloadLength) {
            long value = Util.readLong(currentPacket.payload, payloadOffset);
            payloadOffset += 8;
            return value;
        }

        return Util.readLong(readWrappedBytes(8), 0);
    }

    final void readBytes(byte[] value,
            int valueOffset,
            int valueLength) throws SQLServerException {
        for (int bytesRead = 0; bytesRead < valueLength;) {
            // Ensure that we have a packet to read from.
            if (!ensurePayload())
                throwInvalidTDS();

            // Figure out how many bytes to copy from the current packet
            // (the lesser of the remaining value bytes and the bytes left in the packet).
            int bytesToCopy = valueLength - bytesRead;
            if (bytesToCopy > currentPacket.payloadLength - payloadOffset)
                bytesToCopy = currentPacket.payloadLength - payloadOffset;

            // Copy some bytes from the current packet to the destination value.
            if (logger.isLoggable(Level.FINEST))
                logger.finest(toString() + " Reading " + bytesToCopy + " bytes from offset " + payloadOffset);

            System.arraycopy(currentPacket.payload, payloadOffset, value, valueOffset + bytesRead, bytesToCopy);
            bytesRead += bytesToCopy;
            payloadOffset += bytesToCopy;
        }
    }

    final byte[] readWrappedBytes(int valueLength) throws SQLServerException {
        assert valueLength <= valueBytes.length;
        readBytes(valueBytes, 0, valueLength);
        return valueBytes;
    }

    final Object readDecimal(int valueLength,
            TypeInfo typeInfo,
            JDBCType jdbcType,
            StreamType streamType) throws SQLServerException {
        if (valueLength > valueBytes.length) {
            if (logger.isLoggable(Level.WARNING)) {
                logger.warning(toString() + " Invalid value length:" + valueLength);
            }
            throwInvalidTDS();
        }

        readBytes(valueBytes, 0, valueLength);
        return DDC.convertBigDecimalToObject(Util.readBigDecimal(valueBytes, valueLength, typeInfo.getScale()), jdbcType, streamType);
    }

    final Object readMoney(int valueLength,
            JDBCType jdbcType,
            StreamType streamType) throws SQLServerException {
        BigInteger bi;
        switch (valueLength) {
            case 8: // money
            {
                int intBitsHi = readInt();
                int intBitsLo = readInt();

                if (JDBCType.BINARY == jdbcType) {
                    byte value[] = new byte[8];
                    Util.writeIntBigEndian(intBitsHi, value, 0);
                    Util.writeIntBigEndian(intBitsLo, value, 4);
                    return value;
                }

                bi = BigInteger.valueOf(((long) intBitsHi << 32) | (intBitsLo & 0xFFFFFFFFL));
                break;
            }

            case 4: // smallmoney
                if (JDBCType.BINARY == jdbcType) {
                    byte value[] = new byte[4];
                    Util.writeIntBigEndian(readInt(), value, 0);
                    return value;
                }

                bi = BigInteger.valueOf(readInt());
                break;

            default:
                throwInvalidTDS();
                return null;
        }

        return DDC.convertBigDecimalToObject(new BigDecimal(bi, 4), jdbcType, streamType);
    }

    final Object readReal(int valueLength,
            JDBCType jdbcType,
            StreamType streamType) throws SQLServerException {
        if (4 != valueLength)
            throwInvalidTDS();

        return DDC.convertFloatToObject(Float.intBitsToFloat(readInt()), jdbcType, streamType);
    }

    final Object readFloat(int valueLength,
            JDBCType jdbcType,
            StreamType streamType) throws SQLServerException {
        if (8 != valueLength)
            throwInvalidTDS();

        return DDC.convertDoubleToObject(Double.longBitsToDouble(readLong()), jdbcType, streamType);
    }

    final Object readDateTime(int valueLength,
            Calendar appTimeZoneCalendar,
            JDBCType jdbcType,
            StreamType streamType) throws SQLServerException {
        // Build and return the right kind of temporal object.
        int daysSinceSQLBaseDate;
        int ticksSinceMidnight;
        int msecSinceMidnight;

        switch (valueLength) {
            case 8:
                // SQL datetime is 4 bytes for days since SQL Base Date
                // (January 1, 1900 00:00:00 GMT) and 4 bytes for
                // the number of three hundredths (1/300) of a second
                // since midnight.
                daysSinceSQLBaseDate = readInt();
                ticksSinceMidnight = readInt();

                if (JDBCType.BINARY == jdbcType) {
                    byte value[] = new byte[8];
                    Util.writeIntBigEndian(daysSinceSQLBaseDate, value, 0);
                    Util.writeIntBigEndian(ticksSinceMidnight, value, 4);
                    return value;
                }

                msecSinceMidnight = (ticksSinceMidnight * 10 + 1) / 3; // Convert to msec (1 tick = 1 300th of a sec = 3 msec)
                break;

            case 4:
                // SQL smalldatetime has less precision. It stores 2 bytes
                // for the days since SQL Base Date and 2 bytes for minutes
                // after midnight.
                daysSinceSQLBaseDate = readUnsignedShort();
                ticksSinceMidnight = readUnsignedShort();

                if (JDBCType.BINARY == jdbcType) {
                    byte value[] = new byte[4];
                    Util.writeShortBigEndian((short) daysSinceSQLBaseDate, value, 0);
                    Util.writeShortBigEndian((short) ticksSinceMidnight, value, 2);
                    return value;
                }

                msecSinceMidnight = ticksSinceMidnight * 60 * 1000; // Convert to msec (1 tick = 1 min = 60,000 msec)
                break;

            default:
                throwInvalidTDS();
                return null;
        }

        // Convert the DATETIME/SMALLDATETIME value to the desired Java type.
        return DDC.convertTemporalToObject(jdbcType, SSType.DATETIME, appTimeZoneCalendar, daysSinceSQLBaseDate, msecSinceMidnight, 0); // scale
                                                                                                                                        // (ignored
                                                                                                                                        // for
                                                                                                                                        // fixed-scale
                                                                                                                                        // DATETIME/SMALLDATETIME
                                                                                                                                        // types)
    }

    final Object readDate(int valueLength,
            Calendar appTimeZoneCalendar,
            JDBCType jdbcType) throws SQLServerException {
        if (TDS.DAYS_INTO_CE_LENGTH != valueLength)
            throwInvalidTDS();

        // Initialize the date fields to their appropriate values.
        int localDaysIntoCE = readDaysIntoCE();

        // Convert the DATE value to the desired Java type.
        return DDC.convertTemporalToObject(jdbcType, SSType.DATE, appTimeZoneCalendar, localDaysIntoCE, 0,  // midnight local to app time zone
                0); // scale (ignored for DATE)
    }

    final Object readTime(int valueLength,
            TypeInfo typeInfo,
            Calendar appTimeZoneCalendar,
            JDBCType jdbcType) throws SQLServerException {
        if (TDS.timeValueLength(typeInfo.getScale()) != valueLength)
            throwInvalidTDS();

        // Read the value from the server
        long localNanosSinceMidnight = readNanosSinceMidnight(typeInfo.getScale());

        // Convert the TIME value to the desired Java type.
        return DDC.convertTemporalToObject(jdbcType, SSType.TIME, appTimeZoneCalendar, 0, localNanosSinceMidnight, typeInfo.getScale());
    }

    final Object readDateTime2(int valueLength,
            TypeInfo typeInfo,
            Calendar appTimeZoneCalendar,
            JDBCType jdbcType) throws SQLServerException {
        if (TDS.datetime2ValueLength(typeInfo.getScale()) != valueLength)
            throwInvalidTDS();

        // Read the value's constituent components
        long localNanosSinceMidnight = readNanosSinceMidnight(typeInfo.getScale());
        int localDaysIntoCE = readDaysIntoCE();

        // Convert the DATETIME2 value to the desired Java type.
        return DDC.convertTemporalToObject(jdbcType, SSType.DATETIME2, appTimeZoneCalendar, localDaysIntoCE, localNanosSinceMidnight,
                typeInfo.getScale());
    }

    final Object readDateTimeOffset(int valueLength,
            TypeInfo typeInfo,
            JDBCType jdbcType) throws SQLServerException {
        if (TDS.datetimeoffsetValueLength(typeInfo.getScale()) != valueLength)
            throwInvalidTDS();

        // The nanos since midnight and days into Common Era parts of DATETIMEOFFSET values
        // are in UTC. Use the minutes offset part to convert to local.
        long utcNanosSinceMidnight = readNanosSinceMidnight(typeInfo.getScale());
        int utcDaysIntoCE = readDaysIntoCE();
        int localMinutesOffset = readShort();

        // Convert the DATETIMEOFFSET value to the desired Java type.
        return DDC.convertTemporalToObject(jdbcType, SSType.DATETIMEOFFSET,
                new GregorianCalendar(new SimpleTimeZone(localMinutesOffset * 60 * 1000, ""), Locale.US), utcDaysIntoCE, utcNanosSinceMidnight,
                typeInfo.getScale());
    }

    private int readDaysIntoCE() throws SQLServerException {
        byte value[] = new byte[TDS.DAYS_INTO_CE_LENGTH];
        readBytes(value, 0, value.length);

        int daysIntoCE = 0;
        for (int i = 0; i < value.length; i++)
            daysIntoCE |= ((value[i] & 0xFF) << (8 * i));

        // Theoretically should never encounter a value that is outside of the valid date range
        if (daysIntoCE < 0)
            throwInvalidTDS();

        return daysIntoCE;
    }

    // Scale multipliers used to convert variable-scaled temporal values to a fixed 100ns scale.
    //
    // Using this array is measurably faster than using Math.pow(10, ...)
    private final static int[] SCALED_MULTIPLIERS = {10000000, 1000000, 100000, 10000, 1000, 100, 10, 1};

    private long readNanosSinceMidnight(int scale) throws SQLServerException {
        assert 0 <= scale && scale <= TDS.MAX_FRACTIONAL_SECONDS_SCALE;

        byte value[] = new byte[TDS.nanosSinceMidnightLength(scale)];
        readBytes(value, 0, value.length);

        long hundredNanosSinceMidnight = 0;
        for (int i = 0; i < value.length; i++)
            hundredNanosSinceMidnight |= (value[i] & 0xFFL) << (8 * i);

        hundredNanosSinceMidnight *= SCALED_MULTIPLIERS[scale];

        if (!(0 <= hundredNanosSinceMidnight && hundredNanosSinceMidnight < Nanos.PER_DAY / 100))
            throwInvalidTDS();

        return 100 * hundredNanosSinceMidnight;
    }

    final static String guidTemplate = "NNNNNNNN-NNNN-NNNN-NNNN-NNNNNNNNNNNN";

    final Object readGUID(int valueLength,
            JDBCType jdbcType,
            StreamType streamType) throws SQLServerException {
        // GUIDs must be exactly 16 bytes
        if (16 != valueLength)
            throwInvalidTDS();

        // Read in the GUID's binary value
        byte guid[] = new byte[16];
        readBytes(guid, 0, 16);

        switch (jdbcType) {
            case CHAR:
            case VARCHAR:
            case LONGVARCHAR:
            case GUID: {
                StringBuilder sb = new StringBuilder(guidTemplate.length());
                for (int i = 0; i < 4; i++) {
                    sb.append(Util.hexChars[(guid[3 - i] & 0xF0) >> 4]);
                    sb.append(Util.hexChars[guid[3 - i] & 0x0F]);
                }
                sb.append('-');
                for (int i = 0; i < 2; i++) {
                    sb.append(Util.hexChars[(guid[5 - i] & 0xF0) >> 4]);
                    sb.append(Util.hexChars[guid[5 - i] & 0x0F]);
                }
                sb.append('-');
                for (int i = 0; i < 2; i++) {
                    sb.append(Util.hexChars[(guid[7 - i] & 0xF0) >> 4]);
                    sb.append(Util.hexChars[guid[7 - i] & 0x0F]);
                }
                sb.append('-');
                for (int i = 0; i < 2; i++) {
                    sb.append(Util.hexChars[(guid[8 + i] & 0xF0) >> 4]);
                    sb.append(Util.hexChars[guid[8 + i] & 0x0F]);
                }
                sb.append('-');
                for (int i = 0; i < 6; i++) {
                    sb.append(Util.hexChars[(guid[10 + i] & 0xF0) >> 4]);
                    sb.append(Util.hexChars[guid[10 + i] & 0x0F]);
                }

                try {
                    return DDC.convertStringToObject(sb.toString(), Encoding.UNICODE.charset(), jdbcType, streamType);
                }
                catch (UnsupportedEncodingException e) {
                    MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_errorConvertingValue"));
                    throw new SQLServerException(form.format(new Object[] {"UNIQUEIDENTIFIER", jdbcType}), null, 0, e);
                }
            }

            default: {
                if (StreamType.BINARY == streamType || StreamType.ASCII == streamType)
                    return new ByteArrayInputStream(guid);

                return guid;
            }
        }
    }

    /**
     * Reads a multi-part table name from TDS and returns it as an array of Strings.
     */
    final SQLIdentifier readSQLIdentifier() throws SQLServerException {
        // Multi-part names should have between 1 and 4 parts
        int numParts = readUnsignedByte();
        if (!(1 <= numParts && numParts <= 4))
            throwInvalidTDS();

        // Each part is a length-prefixed Unicode string
        String[] nameParts = new String[numParts];
        for (int i = 0; i < numParts; i++)
            nameParts[i] = readUnicodeString(readUnsignedShort());

        // Build the identifier from the name parts
        SQLIdentifier identifier = new SQLIdentifier();
        identifier.setObjectName(nameParts[numParts - 1]);
        if (numParts >= 2)
            identifier.setSchemaName(nameParts[numParts - 2]);
        if (numParts >= 3)
            identifier.setDatabaseName(nameParts[numParts - 3]);
        if (4 == numParts)
            identifier.setServerName(nameParts[numParts - 4]);

        return identifier;
    }

    final SQLCollation readCollation() throws SQLServerException {
        SQLCollation collation = null;

        try {
            collation = new SQLCollation(this);
        }
        catch (UnsupportedEncodingException e) {
            con.terminate(SQLServerException.DRIVER_ERROR_INVALID_TDS, e.getMessage(), e);
            // not reached
        }

        return collation;
    }

    final void skip(int bytesToSkip) throws SQLServerException {
        assert bytesToSkip >= 0;

        while (bytesToSkip > 0) {
            // Ensure that we have a packet to read from.
            if (!ensurePayload())
                throwInvalidTDS();

            int bytesSkipped = bytesToSkip;
            if (bytesSkipped > currentPacket.payloadLength - payloadOffset)
                bytesSkipped = currentPacket.payloadLength - payloadOffset;

            bytesToSkip -= bytesSkipped;
            payloadOffset += bytesSkipped;
        }
    }

    final void TryProcessFeatureExtAck(boolean featureExtAckReceived) throws SQLServerException {
        // in case of redirection, do not check if TDS_FEATURE_EXTENSION_ACK is received or not.
        if (null != this.con.getRoutingInfo()) {
            return;
        }

        if (isColumnEncryptionSettingEnabled() && !featureExtAckReceived)
            throw new SQLServerException(this, SQLServerException.getErrString("R_AE_NotSupportedByServer"), null, 0, false);
    }
}