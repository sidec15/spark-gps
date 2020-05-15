/**
 * MainTest.java
 */
package com.sdc.spark.spark_gps;

import java.io.File;
import java.net.URISyntaxException;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.Test;

import static org.junit.Assert.*;
import static org.hamcrest.Matchers.*;

/**
 * @author Simone.DeCristofaro
 * May 14, 2020
 */
public class MainTest extends TestWithSparkSession {

    @Test
    public void test() throws Exception {
        
        String input = getResourceFile(getClass(), "/input.csv").getAbsolutePath();
        String output = "target/test/output";
        String format  = "json";
        String bbox = "-180,-90,180,90";
        String local = "local";
        
        String[] args = new String[] {input,output,format,bbox,local};
        
        Main.main(args);
        
        setUp();
        
        String filteredFcdPath = Utils.joinFiles(output, Main.OUTPUT_FILTER);
        Dataset<Row> fcdFilteredDS = getSparkSession().read().format(format).load(filteredFcdPath);
        assertThat(fcdFilteredDS.count(), is(equalTo(16l)));
        
        String trajectoriesPath = Utils.joinFiles(output, Main.OUTPUT_TRAJECTORIES);
        Dataset<Row> trajectoriesDS = getSparkSession().read().format(format).load(trajectoriesPath);
        assertThat(trajectoriesDS.count(), is(equalTo(2l)));
        
    }
    
    public static File getResourceFile(Class<?> clazz, String path) throws URISyntaxException {
        return new File(clazz.getResource(path).toURI());
    }
    
}
