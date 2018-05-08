/*
 * Microsoft JDBC Driver for SQL Server
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

/**
 * TDSPacket provides a mechanism for chaining TDS response packets together in a singly-linked list.
 *
 * Having both the link and the data in the same class allows TDSReader marks (see below) to automatically hold onto exactly as much response data as
 * they need, and no more. Java reference semantics ensure that a mark holds onto its referenced packet and subsequent packets (through next
 * references). When all marked references to a packet go away, the packet, and any linked unmarked packets, can be reclaimed by GC.
 */
final class TDSPacket {
    final byte[] header = new byte[TDS.PACKET_HEADER_SIZE];
    final byte[] payload;
    int payloadLength;
    volatile TDSPacket next;

    final public String toString() {
        return "TDSPacket(SPID:" + Util.readUnsignedShortBigEndian(header, TDS.PACKET_HEADER_SPID) + " Seq:" + header[TDS.PACKET_HEADER_SEQUENCE_NUM]
                + ")";
    }

    TDSPacket(int size) {
        payload = new byte[size];
        payloadLength = 0;
        next = null;
    }

    final boolean isEOM() {
        return TDS.STATUS_BIT_EOM == (header[TDS.PACKET_HEADER_MESSAGE_STATUS] & TDS.STATUS_BIT_EOM);
    }
};