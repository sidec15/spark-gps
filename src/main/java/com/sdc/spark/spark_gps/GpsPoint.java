/**
 * GpsPoint.java
 */
package com.sdc.spark.spark_gps;

import java.io.Serializable;

import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.types.StructType;

/**
 * @author Simone.DeCristofaro
 * May 14, 2020
 */
public class GpsPoint implements Serializable{

    private static final long serialVersionUID = 1L;
    
    private String id;
    private long timestamp;
    private float longitude;
    private float latitude;
    
    public static final Encoder<GpsPoint> ENCODER = Encoders.bean(GpsPoint.class);
    public static final StructType SCHEMA = ENCODER.schema();

    
    /**
     * 
     */
    public GpsPoint() {

        super();
    }



    /**
     * @param id
     * @param timestamp
     * @param longitude
     * @param latitude
     */
    public GpsPoint(String id, long timestamp, float longitude, float latitude) {

        super();
        this.id = id;
        this.timestamp = timestamp;
        this.longitude = longitude;
        this.latitude = latitude;
    }



    
    /**
     * @return the {@link GpsPoint#id}
     */
    public String getId() {
    
        return id;
    }



    
    /**
     * @param id the {@link GpsPoint#id} to set
     */
    public void setId(String id) {
    
        this.id = id;
    }



    
    /**
     * @return the {@link GpsPoint#timestamp}
     */
    public long getTimestamp() {
    
        return timestamp;
    }



    
    /**
     * @param timestamp the {@link GpsPoint#timestamp} to set
     */
    public void setTimestamp(long timestamp) {
    
        this.timestamp = timestamp;
    }



    
    /**
     * @return the {@link GpsPoint#longitude}
     */
    public float getLongitude() {
    
        return longitude;
    }



    
    /**
     * @param longitude the {@link GpsPoint#longitude} to set
     */
    public void setLongitude(float longitude) {
    
        this.longitude = longitude;
    }



    
    /**
     * @return the {@link GpsPoint#latitude}
     */
    public float getLatitude() {
    
        return latitude;
    }



    
    /**
     * @param latitude the {@link GpsPoint#latitude} to set
     */
    public void setLatitude(float latitude) {
    
        this.latitude = latitude;
    }
    
    

}
