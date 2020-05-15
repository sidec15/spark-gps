/**
 * MainTest.java
 */
package com.sdc.spark.spark_gps;

import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;
import java.net.UnknownHostException;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.junit.After;
import org.junit.Before;

/**
 * @author Simone.DeCristofaro
 * May 14, 2020
 */
public abstract class TestWithSparkSession {

    private SparkSession sparkSession;
    private JavaSparkContext javaSparkContext;
    
    
    @Before
    public void setUp() throws Exception {
        setUp("unit-testing-sample");
    }

    @After
    public void tearDown() throws Exception {
        if(javaSparkContext != null)
            javaSparkContext.close();
        if(sparkSession != null)
            sparkSession.close();
    }
    
    protected String getIp() {

        String ip;
        try {
            // needed with spark >= 2.3
            ip = getLocalIp();
        }
        catch (UnknownHostException | SocketException e) {
            throw new RuntimeException("Error obtaining machine ip address", e);
        }
        return ip;
    }
    
    
    public static String getLocalIp() throws UnknownHostException, SocketException {

        try (final DatagramSocket socket = new DatagramSocket()) {
            socket.connect(InetAddress.getByName("8.8.8.8"), 10002);
            return socket.getLocalAddress().getHostAddress();
        }
    }

    
    public void setUp(String appName) {
        sparkSession = SparkSession.builder()
                .master("local[*]").appName(appName)
                // needed with spark >= 2.3
                .config("spark.driver.host", getIp())
                .getOrCreate();

        javaSparkContext = new JavaSparkContext(sparkSession.sparkContext());
        
    }

    
    /**
     * @return the {@link TestWithSparkSession#sparkSession}
     */
    public SparkSession getSparkSession() {
    
        return sparkSession;
    }

    
    /**
     * @return the {@link TestWithSparkSession#javaSparkContext}
     */
    public JavaSparkContext getJavaSparkContext() {
    
        return javaSparkContext;
    }
    
    
    
}
