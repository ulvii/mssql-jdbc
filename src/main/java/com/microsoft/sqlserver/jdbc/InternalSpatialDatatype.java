/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

/**
 * Specifies the spatial data types values.
 */
public enum InternalSpatialDatatype {
    /**
     * Point.
     */
    POINT((byte) 1, "POINT"),
    /**
     * LineString.
     */
    LINESTRING((byte) 2, "LINESTRING"),
    /**
     * Polygon.
     */
    POLYGON((byte) 3, "POLYGON"),
    /**
     * MultiPoint.
     */
    MULTIPOINT((byte) 4, "MULTIPOINT"),
    /**
     * MultiLineString.
     */
    MULTILINESTRING((byte) 5, "MULTILINESTRING"),
    /**
     * MultiPolygon.
     */
    MULTIPOLYGON((byte) 6, "MULTIPOLYGON"),
    /**
     * GeometryCollection.
     */
    GEOMETRYCOLLECTION((byte) 7, "GEOMETRYCOLLECTION"),
    /**
     * CircularString.
     */
    CIRCULARSTRING((byte) 8, "CIRCULARSTRING"),
    /**
     * CompoundCurve.
     */
    COMPOUNDCURVE((byte) 9, "COMPOUNDCURVE"),
    /**
     * CurvePolygon.
     */
    CURVEPOLYGON((byte) 10, "CURVEPOLYGON"),
    /**
     * FullGlobe.
     */
    FULLGLOBE((byte) 11, "FULLGLOBE"),
    /**
     * Invalid type.
     */
    INVALID_TYPE((byte) 0, null);

    private byte typeCode;
    private String typeName;
    private static final InternalSpatialDatatype[] VALUES = values();

    private InternalSpatialDatatype(byte typeCode, String typeName) {
        this.typeCode = typeCode;
        this.typeName = typeName;
    }

    byte getTypeCode() {
        return this.typeCode;
    }

    String getTypeName() {
        return this.typeName;
    }

    static InternalSpatialDatatype valueOf(byte typeCode) {
        for (InternalSpatialDatatype internalType : VALUES) {
            if (internalType.typeCode == typeCode) {
                return internalType;
            }
        }
        return INVALID_TYPE;
    }
}
