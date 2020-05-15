/**
 * Utils.java
 */
package com.sdc.spark.spark_gps;

import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author Simone.DeCristofaro
 * May 14, 2020
 */
public class Utils {
    
    public static final String SLASH = "/";
    public static final String URL_SEPARATOR = SLASH;
    public static final String FILE_SEPARATOR = File.separator;
    public static final String RESOURCE_FILE_SEPARATOR = "/";
    
    public static String joinUrls(String...paths) {
        return join(URL_SEPARATOR, paths);
    }
    
    public static String joinFiles(String...paths) {
        return join(File.separator, paths);
    }
    
    public static String joinResourcesPath(String...paths) {
        return join(RESOURCE_FILE_SEPARATOR, paths);
    }

    
    private static String join(String separator, String...paths) {
        
        List<String> elements = Arrays.asList(paths).stream()
        .map(p -> cleanPath(p, separator))
        .collect(Collectors.toList())
        ;
        return String.join(separator, elements);
        
    }

    private static String cleanPath(String path, String separator) {

        String cleanedPath = path;
        while(cleanedPath.endsWith(separator))
            cleanedPath = cleanedPath.substring(0, cleanedPath.length() - separator.length());
        return cleanedPath;
    }



}
