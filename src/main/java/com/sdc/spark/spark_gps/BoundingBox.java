/**
 * BoundingBox.java
 */
package com.sdc.spark.spark_gps;

import java.io.Serializable;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;

/**
 * @author simone.decristofaro
 *         Apr 4, 2017
 */
public class BoundingBox implements Serializable {

    private static final long serialVersionUID = -4371995287661701333L;

    public static final Encoder<BoundingBox> ENCODER = Encoders.bean(BoundingBox.class);
    
    public static final String MAX_WGS84_BBOX_VALUE = "-180,-90,180,90"; 
    public static final String PARSE_PATTERN_VALUE = "^(?:(-?\\d+(?:\\.\\d*)?),){3}(-?\\d+(?:\\.\\d*)?)$";
    private static final Pattern PARSE_PATTERN = Pattern.compile(PARSE_PATTERN_VALUE);

    private double minX;
    private double minY;
    private double maxX;
    private double maxY;

    /**
     * 
     */
    public BoundingBox() {
        super();
    }

    /**
     * @param minX
     * @param minY
     * @param maxX
     * @param maxY
     */
    public BoundingBox(double minX, double minY, double maxX, double maxY) {
        super();
        this.minX = minX;
        this.minY = minY;
        this.maxX = maxX;
        this.maxY = maxY;
    }


    @Override
    public boolean equals(Object obj) {

        if (obj == null || !obj.getClass().equals(getClass()))
            return false;
        if (obj == this)
            return true;

        BoundingBox other = (BoundingBox) obj;

        return minX == other.minX && minY == other.minY && maxX == other.maxX && maxY == other.maxY;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {

        return String.format("Bounding box: BOTTOM_LEFT = [%s, %s]; UPPER_RIGHT = [%s, %s]", minX, minY, maxX, maxY);

    }

    /**
     * Return <code>true</code> if the specified point is included in the {@link BoundingBox}
     * 
     * @param lon
     * @param lat
     * @return boolean
     */
    public boolean includes(double lon, double lat) {

        return (lon >= minX && lon <= maxX) && (lat >= minY && lat <= maxY);
    }


    /**
     * @return the {@link BoundingBox#minX}
     */
    public double getMinX() {

        return minX;
    }

    /**
     * @param minX
     *            the {@link BoundingBox#minX} to set
     */
    public void setMinX(double minX) {

        this.minX = minX;
    }

    /**
     * @return the {@link BoundingBox#minY}
     */
    public double getMinY() {

        return minY;
    }

    /**
     * @param minY
     *            the {@link BoundingBox#minY} to set
     */
    public void setMinY(double minY) {

        this.minY = minY;
    }

    /**
     * @return the {@link BoundingBox#maxX}
     */
    public double getMaxX() {

        return maxX;
    }

    /**
     * @param maxX
     *            the {@link BoundingBox#maxX} to set
     */
    public void setMaxX(double maxX) {

        this.maxX = maxX;
    }

    /**
     * @return the {@link BoundingBox#maxY}
     */
    public double getMaxY() {

        return maxY;
    }

    /**
     * @param maxY
     *            the {@link BoundingBox#maxY} to set
     */
    public void setMaxY(double maxY) {

        this.maxY = maxY;
    }


    public static BoundingBox parse(String value) {

        if (!validatePattern(value))
            throw new IllegalArgumentException(
                    String.format("For input string: \"%s\". Regex validation is done against pattern: %s", value, PARSE_PATTERN_VALUE));

        String[] v = value.split(",");

        return new BoundingBox(Double.parseDouble(v[0]), Double.parseDouble(v[1]), Double.parseDouble(v[2]), Double.parseDouble(v[3]));
    }

    public static boolean validatePattern(String value) {

        Matcher matcher = PARSE_PATTERN.matcher(value);
        return matcher.find();
    }

}
