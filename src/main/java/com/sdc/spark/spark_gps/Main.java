/**
 * Main.java
 */
package com.sdc.spark.spark_gps;

import java.util.ArrayList;
import java.util.List;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.google.common.collect.Lists;

/**
 * @author Simone.DeCristofaro
 * May 12, 2020
 */
public class Main {
    
    private static final String COL_ID      = "otonomo_id";
    private static final String COL_TS      = "time";
    private static final String COL_LAT     = "latitude";
    private static final String COL_LON     = "longitude";
    
    public static final String OUTPUT_FILTER = "filtered-fcd";
    public static final String OUTPUT_TRAJECTORIES = "trajectories";
    private static final int BREAK_TIME = 300; // seconds
    
    public static void main(String[] args) {

        SparkSession sparkSession = null;
        JavaSparkContext javaSparkContext = null;
        
        try {
            
            // read input parameters
            String fcdInputPath = args[0];
            String baseOutputPath = args[1];
            String outputFormat = args[2];
            String bboxString = args[3]; // we expect a bbox in the following format: "minX,minY,maxX,maxY"
            int nRepetitions = Integer.parseInt(args[4]);
            
            boolean isLocalRun = false;
            if(args.length > 5) {
                isLocalRun = args[5].equals("local");
            }
            
            BoundingBox bbox = BoundingBox.parse(bboxString);
            
            
            SparkConf sparkConf = new SparkConf()
                  .setAppName("spark-gps");
            
            if(isLocalRun)
                sparkConf.setMaster("local[*]");
            
            sparkSession = SparkSession.builder().config(sparkConf).getOrCreate();
            javaSparkContext = new JavaSparkContext(sparkSession.sparkContext());
            
            
            System.out.println(bbox);
            
            
            for(int i = 0; i < nRepetitions; i++) {
                System.out.println(String.format("> Repetion: %d", (i+1)));
                doJob(sparkSession, javaSparkContext, fcdInputPath, baseOutputPath, outputFormat, bbox);
            }
            
        }catch (Exception e) {
            e.printStackTrace();
        }finally {
            if(sparkSession.sparkContext() != null) {
                sparkSession.sparkContext().stop();
            }
            if(javaSparkContext != null) {
                javaSparkContext.close();
            }
            if(sparkSession != null) {
                sparkSession.close();
            }
        }

    }

    /**
     * @param sparkSession
     * @param javaSparkContext
     * @param fcdInputPath
     * @param baseOutputPath
     * @param outputFormat
     * @param bbox
     */
    private static void doJob(SparkSession sparkSession, JavaSparkContext javaSparkContext, String fcdInputPath, String baseOutputPath,
            String outputFormat, BoundingBox bbox) {

        Dataset<Row> fcdDF = sparkSession.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv(fcdInputPath)
                ;
        
        int nPartitions = fcdDF.rdd().getNumPartitions();
        System.out.println(String.format("Set the spark property spark.sql.shuffle.partitions=%d", nPartitions));
        sparkSession.sqlContext().sql(String.format("set spark.sql.shuffle.partitions=%d", nPartitions));
        
        filterFcd(sparkSession, javaSparkContext, fcdDF, fcdInputPath, baseOutputPath, outputFormat, bbox);
        
        createTrajectories(sparkSession, baseOutputPath, outputFormat);
    }

    /**
     * @param sparkSession
     * @param baseOutputPath
     * @param outputFormat
     */
    private static void createTrajectories(SparkSession sparkSession, String baseOutputPath, String outputFormat) {

        String filteredFcdPath = Utils.joinUrls(baseOutputPath, OUTPUT_FILTER);
        Dataset<Row> filteredFcdDF = sparkSession.read().format(outputFormat)
                .option("header", "true")
                .option("inferSchema", "true")
                .load(filteredFcdPath)
                ;
        
        int nPartitions = filteredFcdDF.rdd().getNumPartitions();
        System.out.println(String.format("Set the spark property spark.sql.shuffle.partitions=%d", nPartitions));
        sparkSession.sqlContext().sql(String.format("set spark.sql.shuffle.partitions=%d", nPartitions));
        
        Dataset<Trajectory> trajectoriesDS = filteredFcdDF.select(COL_ID, COL_TS, COL_LON, COL_LAT)
        .map(row -> new GpsPoint(row.getString(row.fieldIndex(COL_ID))
                             , row.getLong(row.fieldIndex(COL_TS)) / 1000
                             , (float) row.getDouble(row.fieldIndex(COL_LON))
                             , (float) row.getDouble(row.fieldIndex(COL_LAT))), GpsPoint.ENCODER)
        .groupByKey(GpsPoint::getId, Encoders.STRING())
        .flatMapGroups((key, it) -> {
            
            List<GpsPoint> gpsRecords = Lists.newArrayList(it);
            gpsRecords.sort((o1, o2) -> Long.compare(o1.getTimestamp(), o2.getTimestamp()));
            
            GpsPoint preavious = null;
            List<GpsPoint> points = new ArrayList<>();
            List<Trajectory> trajectories = new ArrayList<>();
            for (GpsPoint current : gpsRecords) {
                
                if(preavious != null) {
                    
                    if(current.getTimestamp() - preavious.getTimestamp() >= BREAK_TIME) {
                        
                        if(points.size() > 1) {
                            
                            trajectories.add(new Trajectory(key, points));
                        }
                        points = new ArrayList<>();
                        
                    }
                    
                }
                preavious = current;
                points.add(current);
            }
            
            return trajectories.iterator();
            
        }, Trajectory.ENCODER)
        ;
        
        String outputPath = Utils.joinUrls(baseOutputPath, OUTPUT_TRAJECTORIES);
        System.out.println("Persisting trajectories");
        trajectoriesDS.write().option("header", "true").mode("overwrite")
        .format(outputFormat).save(outputPath);
    }

    private static void filterFcd(SparkSession sparkSession, JavaSparkContext javaSparkContext
            , Dataset<Row> fcdDF
            , String fcdInputPath, String baseOutputPath,
            String outputFormat, BoundingBox bbox) {


        Broadcast<BoundingBox> bbocBC = javaSparkContext.broadcast(bbox);
        Dataset<Row> filteredFcdDF = fcdDF.filter(r -> {
           
            double longitude = r.getDouble(r.fieldIndex(COL_LON));
            double latitude = r.getDouble(r.fieldIndex(COL_LAT));
            return bbocBC.getValue().includes(longitude, latitude);
            
        });
        
        String outputPath = Utils.joinUrls(baseOutputPath, OUTPUT_FILTER);
        
        System.out.println("Persisting filtered dataset");
        filteredFcdDF.write().option("header", "true").mode("overwrite")
        .format(outputFormat).save(outputPath);
    }
    
    
}
