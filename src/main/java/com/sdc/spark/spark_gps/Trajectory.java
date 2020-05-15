/**
 * Trajectory.java
 */
package com.sdc.spark.spark_gps;

import java.io.Serializable;
import java.util.List;

/**
 * @author Simone.DeCristofaro
 * May 14, 2020
 */
public class Trajectory implements Serializable{

    private static final long serialVersionUID = 1L;
    
    private String id;
    private List<GpsPoint> points;
    
    public static final org.apache.spark.sql.Encoder<Trajectory> ENCODER = org.apache.spark.sql.Encoders.bean(Trajectory.class);
    public static final org.apache.spark.sql.types.StructType SCHEMA = ENCODER.schema();

    
    /**
     * 
     */
    public Trajectory() {

        super();
    }
    
    /**
     * @param id
     * @param points
     */
    public Trajectory(String id, List<GpsPoint> points) {

        super();
        this.id = id;
        this.points = points;
    }
    
    /**
     * @return the {@link Trajectory#id}
     */
    public String getId() {
    
        return id;
    }
    
    /**
     * @param id the {@link Trajectory#id} to set
     */
    public void setId(String id) {
    
        this.id = id;
    }
    
    /**
     * @return the {@link Trajectory#points}
     */
    public List<GpsPoint> getPoints() {
    
        return points;
    }
    
    /**
     * @param points the {@link Trajectory#points} to set
     */
    public void setPoints(List<GpsPoint> points) {
    
        this.points = points;
    }
    
    
}
