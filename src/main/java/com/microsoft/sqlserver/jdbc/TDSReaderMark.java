/*
 * Microsoft JDBC Driver for SQL Server
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

/**
 * TDSReaderMark encapsulates a fixed position in the response data stream.
 *
 * Response data is quantized into a linked chain of packets. A mark refers to a specific location in a specific packet and relies on Java's reference
 * semantics to automatically keep all subsequent packets accessible until the mark is destroyed.
 */
final class TDSReaderMark {
    final TDSPacket packet;
    final int payloadOffset;

    TDSReaderMark(TDSPacket packet,
            int payloadOffset) {
        this.packet = packet;
        this.payloadOffset = payloadOffset;
    }
}