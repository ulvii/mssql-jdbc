/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import com.microsoft.sqlserver.jdbc.spatialdatatypes.Figure;
import com.microsoft.sqlserver.jdbc.spatialdatatypes.Point;
import com.microsoft.sqlserver.jdbc.spatialdatatypes.Segment;
import com.microsoft.sqlserver.jdbc.spatialdatatypes.Shape;


/**
 * Abstract parent class for Spatial Datatypes that contains common functionalities.
 */

abstract class SQLServerSpatialDatatype {

    /** WKT = Well-Known-Text, WKB = Well-Knwon-Binary */
    /**
     * As a general rule, the ~IndexEnd variables are non-inclusive (i.e. pointIndexEnd = 8 means the shape using it
     * will only go up to the 7th index of the array)
     */
    protected ByteBuffer buffer;
    protected InternalSpatialDatatype internalType;
    protected String wkt;
    protected String wktNoZM;
    protected byte[] wkb;
    protected byte[] wkbNoZM;
    protected int srid;
    protected byte version = 1;
    protected int numberOfPoints;
    protected int numberOfFigures;
    protected int numberOfShapes;
    protected int numberOfSegments;
    protected StringBuffer WKTsb;
    protected StringBuffer WKTsbNoZM;
    protected int currentPointIndex = 0;
    protected int currentFigureIndex = 0;
    protected int currentSegmentIndex = 0;
    protected int currentShapeIndex = 0;
    protected double xValues[];
    protected double yValues[];
    protected double zValues[];
    protected double mValues[];
    protected Figure figures[];
    protected Shape shapes[];
    protected Segment segments[];

    // serialization properties
    protected boolean hasZvalues = false;
    protected boolean hasMvalues = false;
    protected boolean isValid = true;
    protected boolean isSinglePoint = false;
    protected boolean isSingleLineSegment = false;
    protected boolean isLargerThanHemisphere = false;
    protected boolean isNull = true;

    protected final byte FA_INTERIOR_RING = 0;
    protected final byte FA_STROKE = 1;
    protected final byte FA_EXTERIOR_RING = 2;

    protected final byte FA_POINT = 0;
    protected final byte FA_LINE = 1;
    protected final byte FA_ARC = 2;
    protected final byte FA_COMPOSITE_CURVE = 3;

    // WKT to WKB properties
    protected int currentWktPos = 0;
    protected List<Point> pointList = new ArrayList<Point>();
    protected List<Figure> figureList = new ArrayList<Figure>();
    protected List<Shape> shapeList = new ArrayList<Shape>();
    protected List<Segment> segmentList = new ArrayList<Segment>();
    protected byte serializationProperties = 0;

    private final byte SEGMENT_LINE = 0;
    private final byte SEGMENT_ARC = 1;
    private final byte SEGMENT_FIRST_LINE = 2;
    private final byte SEGMENT_FIRST_ARC = 3;

    private final byte hasZvaluesMask = 0b00000001;
    private final byte hasMvaluesMask = 0b00000010;
    private final byte isValidMask = 0b00000100;
    private final byte isSinglePointMask = 0b00001000;
    private final byte isSingleLineSegmentMask = 0b00010000;
    private final byte isLargerThanHemisphereMask = 0b00100000;

    private List<Integer> version_one_shape_indexes = new ArrayList<Integer>();

    /**
     * Serializes the Geogemetry/Geography instance to WKB.
     * 
     * @param excludeZMFromWKB
     *        flag to indicate if Z and M coordinates should be excluded from the WKB representation
     * @param type
     *        Type of Spatial Datatype (Geometry/Geography)
     */
    protected void serializeToWkb(boolean excludeZMFromWKB, SQLServerSpatialDatatype type) {
        ByteBuffer buf = ByteBuffer.allocate(determineWkbCapacity(excludeZMFromWKB));
        createSerializationProperties();

        buf.order(ByteOrder.LITTLE_ENDIAN);
        buf.putInt(srid);
        buf.put(version);
        if (excludeZMFromWKB) {
            byte serializationPropertiesNoZM = serializationProperties;
            if (hasZvalues) {
                serializationPropertiesNoZM -= hasZvaluesMask;
            }

            if (hasMvalues) {
                serializationPropertiesNoZM -= hasMvaluesMask;
            }
            buf.put(serializationPropertiesNoZM);
        } else {
            buf.put(serializationProperties);
        }

        if (!isSinglePoint && !isSingleLineSegment) {
            buf.putInt(numberOfPoints);
        }

        if (type instanceof Geometry) {
            for (int i = 0; i < numberOfPoints; i++) {
                buf.putDouble(xValues[i]);
                buf.putDouble(yValues[i]);
            }
        } else { // Geography
            for (int i = 0; i < numberOfPoints; i++) {
                buf.putDouble(yValues[i]);
                buf.putDouble(xValues[i]);
            }
        }

        if (!excludeZMFromWKB) {
            if (hasZvalues) {
                for (int i = 0; i < numberOfPoints; i++) {
                    buf.putDouble(zValues[i]);
                }
            }

            if (hasMvalues) {
                for (int i = 0; i < numberOfPoints; i++) {
                    buf.putDouble(mValues[i]);
                }
            }
        }

        if (isSinglePoint || isSingleLineSegment) {
            if (excludeZMFromWKB) {
                wkbNoZM = buf.array();
            } else {
                wkb = buf.array();
            }
            return;
        }

        buf.putInt(numberOfFigures);
        for (int i = 0; i < numberOfFigures; i++) {
            buf.put(figures[i].getFiguresAttribute());
            buf.putInt(figures[i].getPointOffset());
        }

        buf.putInt(numberOfShapes);
        for (int i = 0; i < numberOfShapes; i++) {
            buf.putInt(shapes[i].getParentOffset());
            buf.putInt(shapes[i].getFigureOffset());
            buf.put(shapes[i].getOpenGISType());
        }

        if (version == 2 && null != segments) {
            buf.putInt(numberOfSegments);
            for (int i = 0; i < numberOfSegments; i++) {
                buf.put(segments[i].getSegmentType());
            }
        }

        if (excludeZMFromWKB) {
            wkbNoZM = buf.array();
        } else {
            wkb = buf.array();
        }
    }

    /**
     * Deserializes the buffer (that contains WKB representation of Geometry/Geography data), and stores it into
     * multiple corresponding data structures.
     * 
     * @param type
     *        Type of Spatial Datatype (Geography/Geometry)
     * @throws SQLServerException
     *         if an Exception occurs
     */
    protected void parseWkb(SQLServerSpatialDatatype type) throws SQLServerException {
        srid = readInt();
        version = readByte();
        serializationProperties = readByte();

        interpretSerializationPropBytes();
        readNumberOfPoints();
        readPoints(type);

        if (hasZvalues) {
            readZvalues();
        }

        if (hasMvalues) {
            readMvalues();
        }

        if (!(isSinglePoint || isSingleLineSegment)) {
            readNumberOfFigures();
            readFigures();
            readNumberOfShapes();
            readShapes();
        }

        determineInternalType();

        if (buffer.hasRemaining()) {
            if (version == 2 && internalType.getTypeCode() != 8 && internalType.getTypeCode() != 11) {
                readNumberOfSegments();
                readSegments();
            }
        }
    }

    /**
     * Constructs the WKT representation of Geometry/Geography from the deserialized data.
     * 
     * @param sd
     *        the Geometry/Geography instance.
     * @param isd
     *        internal spatial datatype object
     * @param pointIndexEnd
     *        upper bound for reading points
     * @param figureIndexEnd
     *        upper bound for reading figures
     * @param segmentIndexEnd
     *        upper bound for reading segments
     * @param shapeIndexEnd
     *        upper bound for reading shapes
     * @throws SQLServerException
     *         if an exception occurs
     */
    protected void constructWKT(SQLServerSpatialDatatype sd, InternalSpatialDatatype isd, int pointIndexEnd,
            int figureIndexEnd, int segmentIndexEnd, int shapeIndexEnd) throws SQLServerException {
        if (numberOfPoints == 0) {
            if (isd.getTypeCode() == 11) { // FULLGLOBE
                if (sd instanceof Geometry) {
                    MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_illegalTypeForGeometry"));
                    throw new SQLServerException(form.format(new Object[] {"Fullglobe"}), null, 0, null);
                } else {
                    appendToWKTBuffers("FULLGLOBE");
                    return;
                }
            }
            // handle the case of GeometryCollection having empty objects
            if (isd.getTypeCode() == 7 && currentShapeIndex != shapeIndexEnd - 1) {
                currentShapeIndex++;
                appendToWKTBuffers(isd.getTypeName() + "(");
                constructWKT(this, InternalSpatialDatatype.valueOf(shapes[currentShapeIndex].getOpenGISType()),
                        numberOfPoints, numberOfFigures, numberOfSegments, numberOfShapes);
                appendToWKTBuffers(")");
                return;
            }
            appendToWKTBuffers(isd.getTypeName() + " EMPTY");
            return;
        }

        appendToWKTBuffers(isd.getTypeName());
        appendToWKTBuffers("(");

        switch (isd) {
            case POINT:
                constructPointWKT(currentPointIndex);
                break;
            case LINESTRING:
            case CIRCULARSTRING:
                constructLineWKT(currentPointIndex, pointIndexEnd);
                break;
            case POLYGON:
                constructShapeWKT(currentFigureIndex, figureIndexEnd);
                break;
            case MULTIPOINT:
            case MULTILINESTRING:
                constructMultiShapeWKT(currentShapeIndex, shapeIndexEnd);
                break;
            case COMPOUNDCURVE:
                constructCompoundcurveWKT(currentSegmentIndex, segmentIndexEnd, pointIndexEnd);
                break;
            case MULTIPOLYGON:
                constructMultipolygonWKT(currentShapeIndex, shapeIndexEnd);
                break;
            case GEOMETRYCOLLECTION:
                constructGeometryCollectionWKT(shapeIndexEnd);
                break;
            case CURVEPOLYGON:
                constructCurvepolygonWKT(currentFigureIndex, figureIndexEnd, currentSegmentIndex, segmentIndexEnd);
                break;
            default:
                throwIllegalWKTPosition();
        }

        appendToWKTBuffers(")");
    }

    /**
     * Parses WKT and populates the data structures of the Geometry/Geography instance.
     * 
     * @param sd
     *        the Geometry/Geography instance.
     * @param startPos
     *        The index to start from from the WKT.
     * @param parentShapeIndex
     *        The index of the parent's Shape in the shapes array. Used to determine this shape's parent.
     * @param isGeoCollection
     *        flag to indicate if this is part of a GeometryCollection.
     * @throws SQLServerException
     *         if an exception occurs
     */
    protected void parseWKTForSerialization(SQLServerSpatialDatatype sd, int startPos, int parentShapeIndex,
            boolean isGeoCollection) throws SQLServerException {
        // after every iteration of this while loop, the currentWktPosition will be set to the
        // end of the geometry/geography shape, except for the very first iteration of it.
        // This means that there has to be comma (that separates the previous shape with the next shape),
        // or we expect a ')' that will close the entire shape and exit the method.

        while (hasMoreToken()) {
            if (startPos != 0) {
                if (wkt.charAt(currentWktPos) == ')') {
                    return;
                } else if (wkt.charAt(currentWktPos) == ',') {
                    currentWktPos++;
                }
            }

            String nextToken = getNextStringToken().toUpperCase(Locale.US);
            int thisShapeIndex;
            InternalSpatialDatatype isd = InternalSpatialDatatype.INVALID_TYPE;
            try {
                isd = InternalSpatialDatatype.valueOf(nextToken);
            } catch (Exception e) {
                throwIllegalWKTPosition();
            }
            byte fa = 0;

            if (version == 1 && ("CIRCULARSTRING".equals(nextToken) || "COMPOUNDCURVE".equals(nextToken)
                    || "CURVEPOLYGON".equals(nextToken))) {
                version = 2;
            }

            // check for FULLGLOBE before reading the first open bracket, since FULLGLOBE doesn't have one.
            if ("FULLGLOBE".equals(nextToken)) {
                if (sd instanceof Geometry) {
                    MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_illegalTypeForGeometry"));
                    throw new SQLServerException(form.format(new Object[] {"Fullglobe"}), null, 0, null);
                }

                if (startPos != 0) {
                    throwIllegalWKTPosition();
                }

                shapeList.add(new Shape(parentShapeIndex, -1, isd.getTypeCode()));
                isLargerThanHemisphere = true;
                version = 2;
                break;
            }

            // if next keyword is empty, continue the loop.
            if (checkEmptyKeyword(parentShapeIndex, isd, false)) {
                continue;
            }

            readOpenBracket();

            switch (nextToken) {
                case "POINT":
                    if (startPos == 0 && "POINT".equals(nextToken.toUpperCase())) {
                        isSinglePoint = true;
                        internalType = InternalSpatialDatatype.POINT;
                    }

                    if (isGeoCollection) {
                        shapeList.add(new Shape(parentShapeIndex, figureList.size(), isd.getTypeCode()));
                        figureList.add(new Figure(FA_LINE, pointList.size()));
                    }

                    readPointWkt();
                    break;
                case "LINESTRING":
                case "CIRCULARSTRING":
                    shapeList.add(new Shape(parentShapeIndex, figureList.size(), isd.getTypeCode()));
                    fa = isd.getTypeCode() == InternalSpatialDatatype.LINESTRING.getTypeCode() ? FA_STROKE
                                                                                               : FA_EXTERIOR_RING;
                    figureList.add(new Figure(fa, pointList.size()));

                    readLineWkt();

                    if (startPos == 0 && "LINESTRING".equals(nextToken.toUpperCase()) && pointList.size() == 2) {
                        isSingleLineSegment = true;
                    }
                    break;
                case "POLYGON":
                case "MULTIPOINT":
                case "MULTILINESTRING":
                    thisShapeIndex = shapeList.size();
                    shapeList.add(new Shape(parentShapeIndex, figureList.size(), isd.getTypeCode()));

                    readShapeWkt(thisShapeIndex, nextToken);

                    break;
                case "MULTIPOLYGON":
                    thisShapeIndex = shapeList.size();
                    shapeList.add(new Shape(parentShapeIndex, figureList.size(), isd.getTypeCode()));

                    readMultiPolygonWkt(thisShapeIndex, nextToken);

                    break;
                case "COMPOUNDCURVE":
                    shapeList.add(new Shape(parentShapeIndex, figureList.size(), isd.getTypeCode()));
                    figureList.add(new Figure(FA_COMPOSITE_CURVE, pointList.size()));

                    readCompoundCurveWkt(true);

                    break;
                case "CURVEPOLYGON":
                    shapeList.add(new Shape(parentShapeIndex, figureList.size(), isd.getTypeCode()));

                    readCurvePolygon();

                    break;
                case "GEOMETRYCOLLECTION":
                    thisShapeIndex = shapeList.size();
                    shapeList.add(new Shape(parentShapeIndex, figureList.size(), isd.getTypeCode()));

                    parseWKTForSerialization(this, currentWktPos, thisShapeIndex, true);

                    break;
                default:
                    throwIllegalWKTPosition();
            }
            readCloseBracket();
        }

        populateStructures();
    }

    /**
     * Constructs and appends a Point type in WKT form to the stringbuffer. There are two stringbuffers - WKTsb and
     * WKTsbNoZM. WKTsb contains the X, Y, Z and M coordinates, whereas WKTsbNoZM contains only X and Y coordinates.
     * 
     * @param pointIndex
     *        indicates which point to append to the stringbuffer.
     * 
     */
    protected void constructPointWKT(int pointIndex) {
        if (xValues[pointIndex] % 1 == 0) {
            appendToWKTBuffers((int) xValues[pointIndex]);
        } else {
            appendToWKTBuffers(xValues[pointIndex]);
        }
        appendToWKTBuffers(" ");

        if (yValues[pointIndex] % 1 == 0) {
            appendToWKTBuffers((int) yValues[pointIndex]);
        } else {
            appendToWKTBuffers(yValues[pointIndex]);
        }
        appendToWKTBuffers(" ");

        if (hasZvalues && !Double.isNaN(zValues[pointIndex])) {
            if (zValues[pointIndex] % 1 == 0) {
                WKTsb.append((long) zValues[pointIndex]);
            } else {
                WKTsb.append(zValues[pointIndex]);
            }
            WKTsb.append(" ");
        } else if (hasMvalues && !Double.isNaN(mValues[pointIndex])) {
            // Handle the case where the user has POINT (1 2 NULL M) value.
            WKTsb.append("NULL ");
        }

        if (hasMvalues && !Double.isNaN(mValues[pointIndex])) {
            if (mValues[pointIndex] % 1 == 0) {
                WKTsb.append((long) mValues[pointIndex]);
            } else {
                WKTsb.append(mValues[pointIndex]);
            }
            WKTsb.append(" ");
        }

        currentPointIndex++;
        // truncate last space
        WKTsb.setLength(WKTsb.length() - 1);
        WKTsbNoZM.setLength(WKTsbNoZM.length() - 1);
    }

    /**
     * Constructs a line in WKT form.
     * 
     * @param pointStartIndex
     *        .
     * @param pointEndIndex
     *        .
     */
    protected void constructLineWKT(int pointStartIndex, int pointEndIndex) {
        for (int i = pointStartIndex; i < pointEndIndex; i++) {
            constructPointWKT(i);

            // add ', ' to separate points, except for the last point
            if (i != pointEndIndex - 1) {
                appendToWKTBuffers(", ");
            }
        }
    }

    /**
     * Constructs a shape (simple Geometry/Geography entities that are contained within a single bracket) in WKT form.
     * 
     * @param figureStartIndex
     *        .
     * @param figureEndIndex
     *        .
     */
    protected void constructShapeWKT(int figureStartIndex, int figureEndIndex) {
        for (int i = figureStartIndex; i < figureEndIndex; i++) {
            appendToWKTBuffers("(");
            if (i != numberOfFigures - 1) { // not the last figure
                constructLineWKT(figures[i].getPointOffset(), figures[i + 1].getPointOffset());
            } else {
                constructLineWKT(figures[i].getPointOffset(), numberOfPoints);
            }

            if (i != figureEndIndex - 1) {
                appendToWKTBuffers("), ");
            } else {
                appendToWKTBuffers(")");
            }
        }
    }

    /**
     * Constructs a mutli-shape (MultiPoint / MultiLineString) in WKT form.
     * 
     * @param shapeStartIndex
     *        .
     * @param shapeEndIndex
     *        .
     */
    protected void constructMultiShapeWKT(int shapeStartIndex, int shapeEndIndex) {
        for (int i = shapeStartIndex + 1; i < shapeEndIndex; i++) {
            if (shapes[i].getFigureOffset() == -1) { // EMPTY
                appendToWKTBuffers("EMPTY");
            } else {
                constructShapeWKT(shapes[i].getFigureOffset(), shapes[i].getFigureOffset() + 1);
            }
            if (i != shapeEndIndex - 1) {
                appendToWKTBuffers(", ");
            }
        }
    }

    /**
     * Constructs a CompoundCurve in WKT form.
     * 
     * @param segmentStartIndex
     *        .
     * @param segmentEndIndex
     *        .
     * @param pointEndIndex
     *        .
     */
    protected void constructCompoundcurveWKT(int segmentStartIndex, int segmentEndIndex, int pointEndIndex) {
        for (int i = segmentStartIndex; i < segmentEndIndex; i++) {
            byte segment = segments[i].getSegmentType();
            constructSegmentWKT(i, segment, pointEndIndex);

            if (i == segmentEndIndex - 1) {
                appendToWKTBuffers(")");
                break;
            }

            switch (segment) {
                case 0:
                case 2:
                    if (segments[i + 1].getSegmentType() != 0) {
                        appendToWKTBuffers("), ");
                    }
                    break;
                case 1:
                case 3:
                    if (segments[i + 1].getSegmentType() != 1) {
                        appendToWKTBuffers("), ");
                    }
                    break;
                default:
                    return;
            }
        }
    }

    /**
     * Constructs a MultiPolygon in WKT form.
     * 
     * @param shapeStartIndex
     *        .
     * @param shapeEndIndex
     *        .
     */
    protected void constructMultipolygonWKT(int shapeStartIndex, int shapeEndIndex) {
        int figureStartIndex;
        int figureEndIndex;

        for (int i = shapeStartIndex + 1; i < shapeEndIndex; i++) {
            figureEndIndex = figures.length;
            if (shapes[i].getFigureOffset() == -1) { // EMPTY
                appendToWKTBuffers("EMPTY");
                if (!(i == shapeEndIndex - 1)) { // not the last exterior polygon of this multipolygon, add a comma
                    appendToWKTBuffers(", ");
                }
                continue;
            }
            figureStartIndex = shapes[i].getFigureOffset();
            if (i == shapes.length - 1) { // last shape
                figureEndIndex = figures.length;
            } else {
                // look ahead and find the next shape that doesn't have -1 as its figure offset (which signifies EMPTY)
                int tempCurrentShapeIndex = i + 1;
                // We need to iterate this through until the very end of the shapes list, since if the last shape
                // in this MultiPolygon is an EMPTY, it won't know what the correct figureEndIndex would be.
                while (tempCurrentShapeIndex < shapes.length) {
                    if (shapes[tempCurrentShapeIndex].getFigureOffset() == -1) {
                        tempCurrentShapeIndex++;
                        continue;
                    } else {
                        figureEndIndex = shapes[tempCurrentShapeIndex].getFigureOffset();
                        break;
                    }
                }
            }

            appendToWKTBuffers("(");

            for (int j = figureStartIndex; j < figureEndIndex; j++) {
                appendToWKTBuffers("(");// interior ring

                if (j == figures.length - 1) { // last figure
                    constructLineWKT(figures[j].getPointOffset(), numberOfPoints);
                } else {
                    constructLineWKT(figures[j].getPointOffset(), figures[j + 1].getPointOffset());
                }

                if (j == figureEndIndex - 1) { // last polygon of this multipolygon, close off the Multipolygon
                    appendToWKTBuffers(")");
                } else { // not the last polygon, followed by an interior ring
                    appendToWKTBuffers("), ");
                }
            }

            appendToWKTBuffers(")");

            if (!(i == shapeEndIndex - 1)) { // not the last exterior polygon of this multipolygon, add a comma
                appendToWKTBuffers(", ");
            }
        }
    }

    /**
     * Constructs a CurvePolygon in WKT form.
     * 
     * @param figureStartIndex
     *        .
     * @param figureEndIndex
     *        .
     * @param segmentStartIndex
     *        .
     * @param segmentEndIndex
     *        .
     */
    protected void constructCurvepolygonWKT(int figureStartIndex, int figureEndIndex, int segmentStartIndex,
            int segmentEndIndex) {
        for (int i = figureStartIndex; i < figureEndIndex; i++) {
            switch (figures[i].getFiguresAttribute()) {
                case 1: // line
                    appendToWKTBuffers("(");

                    if (i == figures.length - 1) {
                        constructLineWKT(currentPointIndex, numberOfPoints);
                    } else {
                        constructLineWKT(currentPointIndex, figures[i + 1].getPointOffset());
                    }

                    appendToWKTBuffers(")");
                    break;
                case 2: // arc
                    appendToWKTBuffers("CIRCULARSTRING(");

                    if (i == figures.length - 1) {
                        constructLineWKT(currentPointIndex, numberOfPoints);
                    } else {
                        constructLineWKT(currentPointIndex, figures[i + 1].getPointOffset());
                    }

                    appendToWKTBuffers(")");

                    break;
                case 3: // composite curve
                    appendToWKTBuffers("COMPOUNDCURVE(");

                    int pointEndIndex = 0;

                    if (i == figures.length - 1) {
                        pointEndIndex = numberOfPoints;
                    } else {
                        pointEndIndex = figures[i + 1].getPointOffset();
                    }

                    while (currentPointIndex < pointEndIndex) {
                        byte segment = segments[segmentStartIndex].getSegmentType();
                        constructSegmentWKT(segmentStartIndex, segment, pointEndIndex);

                        if (!(currentPointIndex < pointEndIndex)) {
                            appendToWKTBuffers("))");
                        } else {
                            switch (segment) {
                                case 0:
                                case 2:
                                    if (segments[segmentStartIndex + 1].getSegmentType() != 0) {
                                        appendToWKTBuffers("), ");
                                    }
                                    break;
                                case 1:
                                case 3:
                                    if (segments[segmentStartIndex + 1].getSegmentType() != 1) {
                                        appendToWKTBuffers("), ");
                                    }
                                    break;
                                default:
                                    return;
                            }
                        }

                        segmentStartIndex++;
                    }

                    break;
                default:
                    return;
            }

            // Append a comma if this is not the last figure of the shape.
            if (i != figureEndIndex - 1) {
                appendToWKTBuffers(", ");
            }
        }
    }

    /**
     * Constructs a Segment in WKT form. SQL Server re-uses the last point of a segment if the following segment is of
     * type 3 (first arc) or type 2 (first line). This makes sense because the last point of a segment and the first
     * point of the next segment have to match for a valid curve. This means that the code has to look ahead and decide
     * to decrement the currentPointIndex depending on what segment comes next, since it may have been reused (and it's
     * reflected in the array of points)
     * 
     * @param currentSegment
     *        .
     * @param segment
     *        .
     * @param pointEndIndex
     *        .
     */
    protected void constructSegmentWKT(int currentSegment, byte segment, int pointEndIndex) {
        switch (segment) {
            case 0:
                appendToWKTBuffers(", ");
                constructLineWKT(currentPointIndex, currentPointIndex + 1);

                if (currentSegment == segments.length - 1) { // last segment
                    break;
                } else if (segments[currentSegment + 1].getSegmentType() != 0) { // not being followed by another line,
                                                                                 // but not the last segment
                    currentPointIndex = currentPointIndex - 1;
                    incrementPointNumStartIfPointNotReused(pointEndIndex);
                }
                break;

            case 1:
                appendToWKTBuffers(", ");
                constructLineWKT(currentPointIndex, currentPointIndex + 2);

                if (currentSegment == segments.length - 1) { // last segment
                    break;
                } else if (segments[currentSegment + 1].getSegmentType() != 1) { // not being followed by another arc,
                                                                                 // but not the last segment
                    currentPointIndex = currentPointIndex - 1; // only increment pointNumStart by one less than what we
                                                               // should be, since the last
                                                               // point will be reused
                    incrementPointNumStartIfPointNotReused(pointEndIndex);
                }

                break;
            case 2:
                appendToWKTBuffers("(");
                constructLineWKT(currentPointIndex, currentPointIndex + 2);

                if (currentSegment == segments.length - 1) { // last segment
                    break;
                } else if (segments[currentSegment + 1].getSegmentType() != 0) { // not being followed by another line,
                                                                                 // but not the last segment
                    currentPointIndex = currentPointIndex - 1; // only increment pointNumStart by one less than what we
                                                               // should be, since the last
                                                               // point will be reused
                    incrementPointNumStartIfPointNotReused(pointEndIndex);
                }

                break;
            case 3:
                appendToWKTBuffers("CIRCULARSTRING(");
                constructLineWKT(currentPointIndex, currentPointIndex + 3);

                if (currentSegment == segments.length - 1) { // last segment
                    break;
                } else if (segments[currentSegment + 1].getSegmentType() != 1) { // not being followed by another arc
                    currentPointIndex = currentPointIndex - 1; // only increment pointNumStart by one less than what we
                                                               // should be, since the last
                                                               // point will be reused
                    incrementPointNumStartIfPointNotReused(pointEndIndex);
                }

                break;
            default:
                return;
        }
    }

    /**
     * The starting point for constructing a GeometryCollection type in WKT form.
     * 
     * @param shapeEndIndex
     *        .
     * @throws SQLServerException
     *         if an exception occurs
     */
    protected void constructGeometryCollectionWKT(int shapeEndIndex) throws SQLServerException {
        currentShapeIndex++;
        constructGeometryCollectionWKThelper(shapeEndIndex);
    }

    /**
     * Reads Point WKT and adds it to the list of points. This method will read up until and including the comma that
     * may come at the end of the Point WKT.
     * 
     * @throws SQLServerException
     *         if an exception occurs
     */
    protected void readPointWkt() throws SQLServerException {
        int numOfCoordinates = 0;
        double sign;
        double coords[] = new double[4];
        for (int i = 0; i < coords.length; i++) {
            coords[i] = Double.NaN;
        }

        while (numOfCoordinates < 4) {
            sign = 1;
            if (wkt.charAt(currentWktPos) == '-') {
                sign = -1;
                currentWktPos++;
            }

            int startPos = currentWktPos;

            if (wkt.charAt(currentWktPos) == ')') {
                break;
            }

            while (currentWktPos < wkt.length()
                    && (Character.isDigit(wkt.charAt(currentWktPos)) || wkt.charAt(currentWktPos) == '.'
                            || wkt.charAt(currentWktPos) == 'E' || wkt.charAt(currentWktPos) == 'e')) {
                currentWktPos++;
            }

            try {
                coords[numOfCoordinates] = sign * new BigDecimal(wkt.substring(startPos, currentWktPos)).doubleValue();

                if (numOfCoordinates == 2) {
                    hasZvalues = true;
                } else if (numOfCoordinates == 3) {
                    hasMvalues = true;
                }
            } catch (Exception e) { // modify to conversion exception
                // handle NULL case
                // the first check ensures that there is enough space for the wkt to have NULL
                if (wkt.length() > currentWktPos + 3
                        && "null".equalsIgnoreCase(wkt.substring(currentWktPos, currentWktPos + 4))) {
                    coords[numOfCoordinates] = Double.NaN;
                    currentWktPos = currentWktPos + 4;
                } else {
                    throwIllegalWKTPosition();
                }
            }

            numOfCoordinates++;

            skipWhiteSpaces();

            // After skipping white space after the 4th coordinate has been read, the next
            // character has to be either a , or ), or the WKT is invalid.
            if (numOfCoordinates == 4) {
                if (checkSQLLength(currentWktPos + 1) && wkt.charAt(currentWktPos) != ','
                        && wkt.charAt(currentWktPos) != ')') {
                    throwIllegalWKTPosition();
                }
            }

            if (checkSQLLength(currentWktPos + 1) && wkt.charAt(currentWktPos) == ',') {
                // need at least 2 coordinates
                if (numOfCoordinates == 1) {
                    throwIllegalWKTPosition();
                }
                currentWktPos++;
                skipWhiteSpaces();
                break;
            }
            skipWhiteSpaces();
        }

        pointList.add(new Point(coords[0], coords[1], coords[2], coords[3]));
    }

    /**
     * Reads a series of Point types.
     * 
     * @throws SQLServerException
     *         if an exception occurs
     */
    protected void readLineWkt() throws SQLServerException {
        while (currentWktPos < wkt.length() && wkt.charAt(currentWktPos) != ')') {
            readPointWkt();
        }
    }

    /**
     * Reads a shape (simple Geometry/Geography entities that are contained within a single bracket) WKT.
     * 
     * @param parentShapeIndex
     *        shape index of the parent shape that called this method
     * @param nextToken
     *        next string token
     * @throws SQLServerException
     *         if an exception occurs
     */
    protected void readShapeWkt(int parentShapeIndex, String nextToken) throws SQLServerException {
        byte fa = FA_POINT;
        while (currentWktPos < wkt.length() && wkt.charAt(currentWktPos) != ')') {

            // if next keyword is empty, continue the loop.
            // Do not check this for polygon.
            if (!"POLYGON".equals(nextToken)
                    && checkEmptyKeyword(parentShapeIndex, InternalSpatialDatatype.valueOf(nextToken), true)) {
                continue;
            }

            if ("MULTIPOINT".equals(nextToken)) {
                shapeList.add(
                        new Shape(parentShapeIndex, figureList.size(), InternalSpatialDatatype.POINT.getTypeCode()));
            } else if ("MULTILINESTRING".equals(nextToken)) {
                shapeList.add(new Shape(parentShapeIndex, figureList.size(),
                        InternalSpatialDatatype.LINESTRING.getTypeCode()));
            }

            if (version == 1) {
                if ("MULTIPOINT".equals(nextToken)) {
                    fa = FA_STROKE;
                } else if ("MULTILINESTRING".equals(nextToken) || "POLYGON".equals(nextToken)) {
                    fa = FA_EXTERIOR_RING;
                }
                version_one_shape_indexes.add(figureList.size());
            } else if (version == 2) {
                if ("MULTIPOINT".equals(nextToken) || "MULTILINESTRING".equals(nextToken) || "POLYGON".equals(nextToken)
                        || "MULTIPOLYGON".equals(nextToken)) {
                    fa = FA_LINE;
                }
            }

            figureList.add(new Figure(fa, pointList.size()));
            readOpenBracket();
            readLineWkt();
            readCloseBracket();

            skipWhiteSpaces();

            if (checkSQLLength(currentWktPos + 1) && wkt.charAt(currentWktPos) == ',') { // more rings to follow
                readComma();
            } else if (wkt.charAt(currentWktPos) == ')') { // about to exit while loop
                continue;
            } else { // unexpected input
                throwIllegalWKTPosition();
            }
        }
    }

    /**
     * Reads a CurvePolygon WKT.
     * 
     * @throws SQLServerException
     *         if an exception occurs
     */
    protected void readCurvePolygon() throws SQLServerException {
        while (currentWktPos < wkt.length() && wkt.charAt(currentWktPos) != ')') {
            String nextPotentialToken = getNextStringToken().toUpperCase(Locale.US);
            if ("CIRCULARSTRING".equals(nextPotentialToken)) {
                figureList.add(new Figure(FA_ARC, pointList.size()));
                readOpenBracket();
                readLineWkt();
                readCloseBracket();
            } else if ("COMPOUNDCURVE".equals(nextPotentialToken)) {
                figureList.add(new Figure(FA_COMPOSITE_CURVE, pointList.size()));
                readOpenBracket();
                readCompoundCurveWkt(true);
                readCloseBracket();
            } else if (wkt.charAt(currentWktPos) == '(') { // LineString
                figureList.add(new Figure(FA_LINE, pointList.size()));
                readOpenBracket();
                readLineWkt();
                readCloseBracket();
            } else {
                throwIllegalWKTPosition();
            }

            if (checkSQLLength(currentWktPos + 1) && wkt.charAt(currentWktPos) == ',') { // more polygons to follow
                readComma();
            } else if (wkt.charAt(currentWktPos) == ')') { // about to exit while loop
                continue;
            } else { // unexpected input
                throwIllegalWKTPosition();
            }
        }
    }

    /**
     * Reads a MultiPolygon WKT.
     * 
     * @param thisShapeIndex
     *        shape index of current shape
     * @param nextToken
     *        next string token
     * @throws SQLServerException
     *         if an exception occurs
     */
    protected void readMultiPolygonWkt(int thisShapeIndex, String nextToken) throws SQLServerException {
        while (currentWktPos < wkt.length() && wkt.charAt(currentWktPos) != ')') {
            if (checkEmptyKeyword(thisShapeIndex, InternalSpatialDatatype.valueOf(nextToken), true)) {
                continue;
            }
            shapeList.add(new Shape(thisShapeIndex, figureList.size(), InternalSpatialDatatype.POLYGON.getTypeCode())); // exterior
                                                                                                                        // polygon
            readOpenBracket();
            readShapeWkt(thisShapeIndex, nextToken);
            readCloseBracket();

            if (checkSQLLength(currentWktPos + 1) && wkt.charAt(currentWktPos) == ',') { // more polygons to follow
                readComma();
            } else if (wkt.charAt(currentWktPos) == ')') { // about to exit while loop
                continue;
            } else { // unexpected input
                throwIllegalWKTPosition();
            }
        }
    }

    /**
     * Reads a Segment WKT.
     * 
     * @param segmentType
     *        segment type
     * @param isFirstIteration
     *        flag that indicates if this is the first iteration from the loop outside
     * @throws SQLServerException
     *         if an exception occurs
     */
    protected void readSegmentWkt(int segmentType, boolean isFirstIteration) throws SQLServerException {
        segmentList.add(new Segment((byte) segmentType));

        int segmentLength = segmentType;

        // under 2 means 0 or 1 (possible values). 0 (line) has 1 point, and 1 (arc) has 2 points, so increment by one
        if (segmentLength < 2) {
            segmentLength++;
        }

        for (int i = 0; i < segmentLength; i++) {
            // If a segment type of 2 (first line) or 3 (first arc) is not from the very first iteration of the while
            // loop,
            // then the first point has to be a duplicate point from the previous segment, so skip the first point.
            if (i == 0 && !isFirstIteration && segmentType >= 2) {
                skipFirstPointWkt();
            } else {
                readPointWkt();
            }
        }

        if (currentWktPos < wkt.length() && wkt.charAt(currentWktPos) != ')') {
            if (segmentType == SEGMENT_FIRST_ARC || segmentType == SEGMENT_ARC) {
                readSegmentWkt(SEGMENT_ARC, false);
            } else if (segmentType == SEGMENT_FIRST_LINE || segmentType == SEGMENT_LINE) {
                readSegmentWkt(SEGMENT_LINE, false);
            }
        }
    }

    /**
     * Reads a CompoundCurve WKT.
     * 
     * @param isFirstIteration
     *        flag that indicates if this is the first iteration from the loop outside
     * @throws SQLServerException
     *         if an exception occurs
     */
    protected void readCompoundCurveWkt(boolean isFirstIteration) throws SQLServerException {
        while (currentWktPos < wkt.length() && wkt.charAt(currentWktPos) != ')') {
            String nextPotentialToken = getNextStringToken().toUpperCase(Locale.US);
            if ("CIRCULARSTRING".equals(nextPotentialToken)) {
                readOpenBracket();
                readSegmentWkt(SEGMENT_FIRST_ARC, isFirstIteration);
                readCloseBracket();
            } else if (wkt.charAt(currentWktPos) == '(') {// LineString
                readOpenBracket();
                readSegmentWkt(SEGMENT_FIRST_LINE, isFirstIteration);
                readCloseBracket();
            } else {
                throwIllegalWKTPosition();
            }

            isFirstIteration = false;

            if (checkSQLLength(currentWktPos + 1) && wkt.charAt(currentWktPos) == ',') { // more polygons to follow
                readComma();
            } else if (wkt.charAt(currentWktPos) == ')') { // about to exit while loop
                continue;
            } else { // unexpected input
                throwIllegalWKTPosition();
            }
        }
    }

    /**
     * Reads the next string token (usually POINT, LINESTRING, etc.). Then increments currentWktPos to the end of the
     * string token.
     * 
     * @return the next string token
     */
    protected String getNextStringToken() {
        skipWhiteSpaces();
        int endIndex = currentWktPos;
        while (endIndex < wkt.length() && Character.isLetter(wkt.charAt(endIndex))) {
            endIndex++;
        }
        int temp = currentWktPos;
        currentWktPos = endIndex;
        skipWhiteSpaces();

        return wkt.substring(temp, endIndex);
    }

    /**
     * Populates the various data structures contained within the Geometry/Geography instance.
     */
    protected void populateStructures() {
        if (pointList.size() > 0) {
            xValues = new double[pointList.size()];
            yValues = new double[pointList.size()];

            for (int i = 0; i < pointList.size(); i++) {
                xValues[i] = pointList.get(i).getX();
                yValues[i] = pointList.get(i).getY();
            }

            if (hasZvalues) {
                zValues = new double[pointList.size()];
                for (int i = 0; i < pointList.size(); i++) {
                    zValues[i] = pointList.get(i).getZ();
                }
            }

            if (hasMvalues) {
                mValues = new double[pointList.size()];
                for (int i = 0; i < pointList.size(); i++) {
                    mValues[i] = pointList.get(i).getM();
                }
            }
        }

        // if version is 2, then we need to check for potential shapes (polygon & multi-shapes) that were
        // given their figure attributes as if it was version 1, since we don't know what would be the
        // version of the geometry/geography before we parse the entire WKT.
        if (version == 2) {
            for (int i = 0; i < version_one_shape_indexes.size(); i++) {
                figureList.get(version_one_shape_indexes.get(i)).setFiguresAttribute((byte) 1);
            }
        }

        if (figureList.size() > 0) {
            figures = new Figure[figureList.size()];

            for (int i = 0; i < figureList.size(); i++) {
                figures[i] = figureList.get(i);
            }
        }

        // There is an edge case of empty GeometryCollections being inside other GeometryCollections. In this case,
        // the figure offset of the very first shape (GeometryCollections) has to be -1, but this is not possible to
        // know until
        // We've parsed through the entire WKT and confirmed that there are 0 points.
        // Therefore, if so, we make the figure offset of the first shape to be -1.
        if (pointList.size() == 0 && shapeList.size() > 0 && shapeList.get(0).getOpenGISType() == 7) {
            shapeList.get(0).setFigureOffset(-1);
        }

        if (shapeList.size() > 0) {
            shapes = new Shape[shapeList.size()];

            for (int i = 0; i < shapeList.size(); i++) {
                shapes[i] = shapeList.get(i);
            }
        }

        if (segmentList.size() > 0) {
            segments = new Segment[segmentList.size()];

            for (int i = 0; i < segmentList.size(); i++) {
                segments[i] = segmentList.get(i);
            }
        }

        numberOfPoints = pointList.size();
        numberOfFigures = figureList.size();
        numberOfShapes = shapeList.size();
        numberOfSegments = segmentList.size();
    }

    protected void readOpenBracket() throws SQLServerException {
        skipWhiteSpaces();
        if (wkt.charAt(currentWktPos) == '(') {
            currentWktPos++;
            skipWhiteSpaces();
        } else {
            throwIllegalWKTPosition();
        }
    }

    protected void readCloseBracket() throws SQLServerException {
        skipWhiteSpaces();
        if (wkt.charAt(currentWktPos) == ')') {
            currentWktPos++;
            skipWhiteSpaces();
        } else {
            throwIllegalWKTPosition();
        }
    }

    protected boolean hasMoreToken() {
        skipWhiteSpaces();
        return currentWktPos < wkt.length();
    }

    protected void createSerializationProperties() {
        serializationProperties = 0;
        if (hasZvalues) {
            serializationProperties += hasZvaluesMask;
        }

        if (hasMvalues) {
            serializationProperties += hasMvaluesMask;
        }

        if (isValid) {
            serializationProperties += isValidMask;
        }

        if (isSinglePoint) {
            serializationProperties += isSinglePointMask;
        }

        if (isSingleLineSegment) {
            serializationProperties += isSingleLineSegmentMask;
        }

        if (version == 2) {
            if (isLargerThanHemisphere) {
                serializationProperties += isLargerThanHemisphereMask;
            }
        }
    }

    protected int determineWkbCapacity(boolean excludeZMFromWKB) {
        int totalSize = 0;

        totalSize += 6; // SRID + version + SerializationPropertiesByte

        if (isSinglePoint || isSingleLineSegment) {
            totalSize += 16 * numberOfPoints;

            if (!excludeZMFromWKB) {
                if (hasZvalues) {
                    totalSize += 8 * numberOfPoints;
                }

                if (hasMvalues) {
                    totalSize += 8 * numberOfPoints;
                }
            }

            return totalSize;
        }

        int pointSize = 16;
        if (!excludeZMFromWKB) {
            if (hasZvalues) {
                pointSize += 8;
            }

            if (hasMvalues) {
                pointSize += 8;
            }
        }

        totalSize += 12; // 4 bytes for 3 ints, each representing the number of points, shapes and figures
        totalSize += numberOfPoints * pointSize;
        totalSize += numberOfFigures * 5;
        totalSize += numberOfShapes * 9;

        if (version == 2) {
            totalSize += 4; // 4 bytes for 1 int, representing the number of segments
            totalSize += numberOfSegments;
        }

        return totalSize;
    }

    /**
     * Append the data to both stringbuffers.
     * 
     * @param o
     *        data to append to the stringbuffers.
     */
    protected void appendToWKTBuffers(Object o) {
        WKTsb.append(o);
        WKTsbNoZM.append(o);
    }

    protected void interpretSerializationPropBytes() {
        hasZvalues = (serializationProperties & hasZvaluesMask) != 0;
        hasMvalues = (serializationProperties & hasMvaluesMask) != 0;
        isValid = (serializationProperties & isValidMask) != 0;
        isSinglePoint = (serializationProperties & isSinglePointMask) != 0;
        isSingleLineSegment = (serializationProperties & isSingleLineSegmentMask) != 0;
        isLargerThanHemisphere = (serializationProperties & isLargerThanHemisphereMask) != 0;
    }

    protected void readNumberOfPoints() throws SQLServerException {
        if (isSinglePoint) {
            numberOfPoints = 1;
        } else if (isSingleLineSegment) {
            numberOfPoints = 2;
        } else {
            numberOfPoints = readInt();
            checkNegSize(numberOfPoints);
        }
    }

    protected void readZvalues() throws SQLServerException {
        zValues = new double[numberOfPoints];
        for (int i = 0; i < numberOfPoints; i++) {
            zValues[i] = readDouble();
        }
    }

    protected void readMvalues() throws SQLServerException {
        mValues = new double[numberOfPoints];
        for (int i = 0; i < numberOfPoints; i++) {
            mValues[i] = readDouble();
        }
    }

    protected void readNumberOfFigures() throws SQLServerException {
        numberOfFigures = readInt();
        checkNegSize(numberOfFigures);
    }

    protected void readFigures() throws SQLServerException {
        byte fa;
        int po;
        figures = new Figure[numberOfFigures];
        for (int i = 0; i < numberOfFigures; i++) {
            fa = readByte();
            po = readInt();
            figures[i] = new Figure(fa, po);
        }
    }

    protected void readNumberOfShapes() throws SQLServerException {
        numberOfShapes = readInt();
        checkNegSize(numberOfShapes);
    }

    protected void readShapes() throws SQLServerException {
        int po;
        int fo;
        byte ogt;
        shapes = new Shape[numberOfShapes];
        for (int i = 0; i < numberOfShapes; i++) {
            po = readInt();
            fo = readInt();
            ogt = readByte();
            shapes[i] = new Shape(po, fo, ogt);
        }
    }

    protected void readNumberOfSegments() throws SQLServerException {
        numberOfSegments = readInt();
        checkNegSize(numberOfSegments);
    }

    protected void readSegments() throws SQLServerException {
        byte st;
        segments = new Segment[numberOfSegments];
        for (int i = 0; i < numberOfSegments; i++) {
            st = readByte();
            segments[i] = new Segment(st);
        }
    }

    protected void determineInternalType() {
        if (isSinglePoint) {
            internalType = InternalSpatialDatatype.POINT;
        } else if (isSingleLineSegment) {
            internalType = InternalSpatialDatatype.LINESTRING;
        } else {
            internalType = InternalSpatialDatatype.valueOf(shapes[0].getOpenGISType());
        }
    }

    protected boolean checkEmptyKeyword(int parentShapeIndex, InternalSpatialDatatype isd,
            boolean isInsideAnotherShape) throws SQLServerException {
        String potentialEmptyKeyword = getNextStringToken().toUpperCase(Locale.US);
        if ("EMPTY".equals(potentialEmptyKeyword)) {

            byte typeCode = 0;

            if (isInsideAnotherShape) {
                byte parentTypeCode = isd.getTypeCode();
                if (parentTypeCode == 4) { // MultiPoint
                    typeCode = InternalSpatialDatatype.POINT.getTypeCode();
                } else if (parentTypeCode == 5) { // MultiLineString
                    typeCode = InternalSpatialDatatype.LINESTRING.getTypeCode();
                } else if (parentTypeCode == 6) { // MultiPolygon
                    typeCode = InternalSpatialDatatype.POLYGON.getTypeCode();
                } else if (parentTypeCode == 7) { // GeometryCollection
                    typeCode = InternalSpatialDatatype.GEOMETRYCOLLECTION.getTypeCode();
                } else {
                    String strError = SQLServerException.getErrString("R_illegalWKT");
                    throw new SQLServerException(strError, null, 0, null);
                }
            } else {
                typeCode = isd.getTypeCode();
            }

            shapeList.add(new Shape(parentShapeIndex, -1, typeCode));
            skipWhiteSpaces();
            if (currentWktPos < wkt.length() && wkt.charAt(currentWktPos) == ',') {
                currentWktPos++;
                skipWhiteSpaces();
            }
            return true;
        }

        if (!"".equals(potentialEmptyKeyword)) {
            throwIllegalWKTPosition();
        }
        return false;
    }

    protected void throwIllegalWKT() throws SQLServerException {
        String strError = SQLServerException.getErrString("R_illegalWKT");
        throw new SQLServerException(strError, null, 0, null);
    }

    protected void throwIllegalWKB() throws SQLServerException {
        MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_ParsingError"));
        Object[] msgArgs = {JDBCType.VARBINARY};
        throw new SQLServerException(this, form.format(msgArgs), null, 0, false);
    }

    private void incrementPointNumStartIfPointNotReused(int pointEndIndex) {
        // We need to increment PointNumStart if the last point was actually not re-used in the points array.
        // 0 for pointNumEnd indicates that this check is not applicable.
        if (currentPointIndex + 1 >= pointEndIndex) {
            currentPointIndex++;
        }
    }

    /**
     * Helper used for resurcive iteration for constructing GeometryCollection in WKT form.
     * 
     * @param shapeEndIndex
     *        .
     * @throws SQLServerException
     *         if an exception occurs
     */
    private void constructGeometryCollectionWKThelper(int shapeEndIndex) throws SQLServerException {
        // phase 1: assume that there is no multi - stuff and no geometrycollection
        while (currentShapeIndex < shapeEndIndex) {
            InternalSpatialDatatype isd = InternalSpatialDatatype.valueOf(shapes[currentShapeIndex].getOpenGISType());

            int figureIndex = shapes[currentShapeIndex].getFigureOffset();
            int pointIndexEnd = numberOfPoints;
            int figureIndexEnd = numberOfFigures;
            int segmentIndexEnd = numberOfSegments;
            int shapeIndexEnd = numberOfShapes;
            int figureIndexIncrement = 0;
            int segmentIndexIncrement = 0;
            int shapeIndexIncrement = 0;
            int localCurrentSegmentIndex = 0;
            int localCurrentShapeIndex = 0;

            switch (isd) {
                case POINT:
                    figureIndexIncrement++;
                    currentShapeIndex++;
                    break;
                case LINESTRING:
                case CIRCULARSTRING:
                    figureIndexIncrement++;
                    currentShapeIndex++;
                    pointIndexEnd = figures[figureIndex + 1].getPointOffset();
                    break;
                case POLYGON:
                case CURVEPOLYGON:
                    if (currentShapeIndex < shapes.length - 1) {
                        figureIndexEnd = shapes[currentShapeIndex + 1].getFigureOffset();
                    }

                    figureIndexIncrement = figureIndexEnd - currentFigureIndex;
                    currentShapeIndex++;

                    // Needed to keep track of which segment we are at, inside the for loop
                    localCurrentSegmentIndex = currentSegmentIndex;

                    if (isd.equals(InternalSpatialDatatype.CURVEPOLYGON)) {
                        // assume Version 2

                        for (int i = currentFigureIndex; i < figureIndexEnd; i++) {
                            // Only Compoundcurves (with figure attribute 3) can have segments
                            if (figures[i].getFiguresAttribute() == 3) {

                                int pointOffsetEnd;
                                if (i == figures.length - 1) {
                                    pointOffsetEnd = numberOfPoints;
                                } else {
                                    pointOffsetEnd = figures[i + 1].getPointOffset();
                                }

                                int increment = calculateSegmentIncrement(localCurrentSegmentIndex,
                                        pointOffsetEnd - figures[i].getPointOffset());

                                segmentIndexIncrement = segmentIndexIncrement + increment;
                                localCurrentSegmentIndex = localCurrentSegmentIndex + increment;
                            }
                        }
                    }

                    segmentIndexEnd = localCurrentSegmentIndex;

                    break;
                case MULTIPOINT:
                case MULTILINESTRING:
                case MULTIPOLYGON:
                    // Multipoint and MultiLineString can go on for multiple Shapes, but eventually
                    // the parentOffset will signal the end of the object, or it's reached the end of the
                    // shapes array.
                    // There is also no possibility that a MultiPoint or MultiLineString would branch
                    // into another parent.

                    int thisShapesParentOffset = shapes[currentShapeIndex].getParentOffset();

                    int tempShapeIndex = currentShapeIndex;

                    // Increment shapeStartIndex to account for the shape index that either Multipoint, MultiLineString
                    // or MultiPolygon takes up
                    tempShapeIndex++;
                    while (tempShapeIndex < shapes.length
                            && shapes[tempShapeIndex].getParentOffset() != thisShapesParentOffset) {
                        if (!(tempShapeIndex == shapes.length - 1) && // last iteration, don't check for
                                                                      // shapes[tempShapeIndex + 1]
                                !(shapes[tempShapeIndex + 1].getFigureOffset() == -1)) { // disregard EMPTY cases
                            figureIndexEnd = shapes[tempShapeIndex + 1].getFigureOffset();
                        }
                        tempShapeIndex++;
                    }

                    figureIndexIncrement = figureIndexEnd - currentFigureIndex;
                    shapeIndexIncrement = tempShapeIndex - currentShapeIndex;
                    shapeIndexEnd = tempShapeIndex;
                    break;
                case GEOMETRYCOLLECTION:
                    appendToWKTBuffers(isd.getTypeName());

                    // handle Empty GeometryCollection cases
                    if (shapes[currentShapeIndex].getFigureOffset() == -1) {
                        appendToWKTBuffers(" EMPTY");
                        currentShapeIndex++;
                        if (currentShapeIndex < shapeEndIndex) {
                            appendToWKTBuffers(", ");
                        }
                        continue;
                    }

                    appendToWKTBuffers("(");

                    int geometryCollectionParentIndex = shapes[currentShapeIndex].getParentOffset();

                    // Needed to keep track of which shape we are at, inside the for loop
                    localCurrentShapeIndex = currentShapeIndex;

                    while (localCurrentShapeIndex < shapes.length - 1
                            && shapes[localCurrentShapeIndex + 1].getParentOffset() > geometryCollectionParentIndex) {
                        localCurrentShapeIndex++;
                    }
                    // increment localCurrentShapeIndex one more time since it will be used as a shapeEndIndex parameter
                    // for constructGeometryCollectionWKT, and the shapeEndIndex parameter is used non-inclusively
                    localCurrentShapeIndex++;

                    currentShapeIndex++;
                    constructGeometryCollectionWKThelper(localCurrentShapeIndex);

                    if (currentShapeIndex < shapeEndIndex) {
                        appendToWKTBuffers("), ");
                    } else {
                        appendToWKTBuffers(")");
                    }

                    continue;
                case COMPOUNDCURVE:
                    if (currentFigureIndex == figures.length - 1) {
                        pointIndexEnd = numberOfPoints;
                    } else {
                        pointIndexEnd = figures[currentFigureIndex + 1].getPointOffset();
                    }

                    int increment = calculateSegmentIncrement(currentSegmentIndex,
                            pointIndexEnd - figures[currentFigureIndex].getPointOffset());

                    segmentIndexIncrement = increment;
                    segmentIndexEnd = currentSegmentIndex + increment;
                    figureIndexIncrement++;
                    currentShapeIndex++;
                    break;
                case FULLGLOBE:
                    appendToWKTBuffers("FULLGLOBE");
                    break;
                default:
                    break;
            }

            constructWKT(this, isd, pointIndexEnd, figureIndexEnd, segmentIndexEnd, shapeIndexEnd);
            currentFigureIndex = currentFigureIndex + figureIndexIncrement;
            currentSegmentIndex = currentSegmentIndex + segmentIndexIncrement;
            currentShapeIndex = currentShapeIndex + shapeIndexIncrement;

            if (currentShapeIndex < shapeEndIndex) {
                appendToWKTBuffers(", ");
            }
        }
    }

    /**
     * Calculates how many segments will be used by this shape. Needed to determine when the shape that uses segments
     * (e.g. CompoundCurve) needs to stop reading in cases where the CompoundCurve is included as part of
     * GeometryCollection.
     * 
     * @param segmentStart
     *        .
     * @param pointDifference
     *        number of points that were assigned to this segment to be used.
     * @return the number of segments that will be used by this shape.
     */
    private int calculateSegmentIncrement(int segmentStart, int pointDifference) {

        int segmentIncrement = 0;

        while (pointDifference > 0) {
            switch (segments[segmentStart].getSegmentType()) {
                case 0:
                    pointDifference = pointDifference - 1;

                    if (segmentStart == segments.length - 1 || pointDifference < 1) { // last segment
                        break;
                    } else if (segments[segmentStart + 1].getSegmentType() != 0) { // one point will be reused
                        pointDifference = pointDifference + 1;
                    }
                    break;
                case 1:
                    pointDifference = pointDifference - 2;

                    if (segmentStart == segments.length - 1 || pointDifference < 1) { // last segment
                        break;
                    } else if (segments[segmentStart + 1].getSegmentType() != 1) { // one point will be reused
                        pointDifference = pointDifference + 1;
                    }
                    break;
                case 2:
                    pointDifference = pointDifference - 2;

                    if (segmentStart == segments.length - 1 || pointDifference < 1) { // last segment
                        break;
                    } else if (segments[segmentStart + 1].getSegmentType() != 0) { // one point will be reused
                        pointDifference = pointDifference + 1;
                    }
                    break;
                case 3:
                    pointDifference = pointDifference - 3;

                    if (segmentStart == segments.length - 1 || pointDifference < 1) { // last segment
                        break;
                    } else if (segments[segmentStart + 1].getSegmentType() != 1) { // one point will be reused
                        pointDifference = pointDifference + 1;
                    }
                    break;
                default:
                    return segmentIncrement;
            }
            segmentStart++;
            segmentIncrement++;
        }

        return segmentIncrement;
    }

    private void skipFirstPointWkt() {
        int numOfCoordinates = 0;

        while (numOfCoordinates < 4) {
            if (wkt.charAt(currentWktPos) == '-') {
                currentWktPos++;
            }

            if (wkt.charAt(currentWktPos) == ')') {
                break;
            }

            while (currentWktPos < wkt.length()
                    && (Character.isDigit(wkt.charAt(currentWktPos)) || wkt.charAt(currentWktPos) == '.'
                            || wkt.charAt(currentWktPos) == 'E' || wkt.charAt(currentWktPos) == 'e')) {
                currentWktPos++;
            }

            skipWhiteSpaces();
            if (wkt.charAt(currentWktPos) == ',') {
                currentWktPos++;
                skipWhiteSpaces();
                numOfCoordinates++;
                break;
            }
            skipWhiteSpaces();

            numOfCoordinates++;
        }
    }

    private void readComma() throws SQLServerException {
        skipWhiteSpaces();
        if (wkt.charAt(currentWktPos) == ',') {
            currentWktPos++;
            skipWhiteSpaces();
        } else {
            throwIllegalWKTPosition();
        }
    }

    private void skipWhiteSpaces() {
        while (currentWktPos < wkt.length() && Character.isWhitespace(wkt.charAt(currentWktPos))) {
            currentWktPos++;
        }
    }

    private void checkNegSize(int num) throws SQLServerException {
        if (num < 0) {
            throwIllegalWKB();
        }
    }

    private void readPoints(SQLServerSpatialDatatype type) throws SQLServerException {
        xValues = new double[numberOfPoints];
        yValues = new double[numberOfPoints];

        if (type instanceof Geometry) {
            for (int i = 0; i < numberOfPoints; i++) {
                xValues[i] = readDouble();
                yValues[i] = readDouble();
            }
        } else { // Geography
            for (int i = 0; i < numberOfPoints; i++) {
                yValues[i] = readDouble();
                xValues[i] = readDouble();
            }
        }
    }

    private void checkBuffer(int i) throws SQLServerException {
        if (buffer.remaining() < i) {
            throwIllegalWKB();
        }
    }

    private boolean checkSQLLength(int length) throws SQLServerException {
        if (null == wkt || wkt.length() < length) {
            throwIllegalWKTPosition();
        }
        return true;
    }

    private void throwIllegalWKTPosition() throws SQLServerException {
        MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_illegalWKTposition"));
        throw new SQLServerException(form.format(new Object[] {currentWktPos}), null, 0, null);
    }

    protected byte readByte() throws SQLServerException {
        checkBuffer(1);
        return buffer.get();
    }

    protected int readInt() throws SQLServerException {
        checkBuffer(4);
        return buffer.getInt();
    }

    protected double readDouble() throws SQLServerException {
        checkBuffer(8);
        return buffer.getDouble();
    }

    // Allow retrieval of internal structures
    public List<Point> getPointList() {
        return pointList;
    }

    public List<Figure> getFigureList() {
        return figureList;
    }

    public List<Shape> getShapeList() {
        return shapeList;
    }

    public List<Segment> getSegmentList() {
        return segmentList;
    }
}
