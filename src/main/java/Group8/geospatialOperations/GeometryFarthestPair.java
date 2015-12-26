package Group8.geospatialOperations;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;


import com.vividsolutions.jts.algorithm.ConvexHull;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;

/*
*
* Farthest_Pair.java is a class finds the fairathest pair of x and y coordinate points
* using Hadoop distributed file system and Apache Spark RDD.
* It uses as a convex hull to get only the points on the outside of the largest convex polygon
* for the given set of points, then it uses a nested loop to find the farthest pair of the 
* remaining points.
*
* @version 1 2015
* @author Garrett Gutierrez
*
*/
public class GeometryFarthestPair implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private static List<Coordinate> farthestPair;
		
	/**
     * Compare two Coordinates in ascending order based first on x coordinate then y coordinate.
     * @param a the first Coordinate
     * @param b the second Coordinate
     * @return int if a is less than be in x and y then return -1, if a is equal to be in x and y then return 0
     * if a is greater than be in x and y then return 1
     */
	public static int compareTo(Coordinate a, Coordinate b) {
		if(a.x > b.x) {
			return 1;
		} else if (a.x < b.x) {
			return -1;
		} else {
			if (a.x > b.x){
				return 1;
			} else if (a.x < b.x) {
				return -1;
			} else {
				return 0;
			}
		}
	}
	
	/**
     * Find the farthest pair of points by comparing the distance of each pair of points in the coordinate list
     * @param Coordinates is a list of Coordinates with x and y values
     * @return List<Coordinate> the resulting farthest pair with greatest euclidean distance
     */
	public static List<Coordinate> farthest_pair(List<Coordinate> Coordinates) {
		int i = 0, j = 0;
		int size = Coordinates.size();
		int coordinate1 = 0;
		int coordinate2 = 0;
		double currentDistance = 0.0, temp = 0.0;
		
			farthestPair = new ArrayList<Coordinate>();
			while (i < size) {
				j = i + 1;
				while (j < size) {
					
					// Get the euclidean distance of the current pair of Coordinates
					temp = Math.sqrt(Math.pow(Coordinates.get(i).x - Coordinates.get(j).x , 2) + Math.pow(Coordinates.get(i).y  - Coordinates.get(j).y, 2));		
					if (temp > currentDistance) {
						
						// Keep track of the pair of Coordinates with current greatest Euclidean distance
						currentDistance = temp;
						coordinate1 = i;
						coordinate2 = j;
					}
					j++;
				}
				i++;
			}
			farthestPair.add(Coordinates.get(coordinate1));
			farthestPair.add(Coordinates.get(coordinate2));
			return farthestPair;
		}
	
	/**
     * Find the farthest pair of points given a list of x and y coordinates in a text file
     * using a convex hull and a nested loop algorithm using JavaRDD to distribute the load to partitions
     * @param args[0] is the file of the input file in the format x,y
     * @param args[q] is the file path where the output will be written in sorted form according to x then y coordinate
     * in the format of x,y
     * @return void
     */
	public static void main(String[] args){
		String logFile  = args[0];
		JavaRDD<String> logData = null;
		JavaRDD<Coordinate> parseCoordinates = null;
		JavaRDD<Coordinate> initial = null;
		JavaRDD<Coordinate> local = null;
		JavaRDD<Coordinate> convergedResult = null;
		SparkConf conf = new SparkConf().setAppName("Group8-FarthestPair");
	    JavaSparkContext sc = new JavaSparkContext(conf);

		// Load the file
		logData = sc.textFile(logFile);
		
		// Start parsing the file into x and y values
		parseCoordinates = logData.map(new Function<String,  Coordinate>() {
			private static final long serialVersionUID = 1L;

			public Coordinate call(String s) {
				String[] CoordinateParser = null;
				CoordinateParser = s.split(",");
				return new Coordinate(Double.parseDouble(CoordinateParser[0]), Double.parseDouble(CoordinateParser[1]));

			}
		});
		
		// Partition Coordinates on multiple workers and perform local convex hull on each worker
		initial = parseCoordinates.mapPartitions(new FlatMapFunction<Iterator<Coordinate>, Coordinate>() {
			private static final long serialVersionUID = 1L;
			public Iterable<Coordinate> call(Iterator<Coordinate> CoordinateList) throws Exception {
				Coordinate temp = null;
				Coordinate[] pointArray = null;
				Geometry convexHullResult = null;
				GeometryFactory convexHullCreator = null;
				List<Coordinate> allCoordinates = new ArrayList<Coordinate>();
				List<Coordinate> convexHullPoint = null;
				
				
				// Go through Coordinate in the RDD and add it to a list
				while (CoordinateList.hasNext()) {
					temp = CoordinateList.next();
					allCoordinates.add(temp);
				}
				
				// Return  the result of the local convex hull of each partition
				pointArray = new Coordinate[allCoordinates.size()];
				allCoordinates.toArray(pointArray);
				convexHullCreator = new GeometryFactory();
				ConvexHull computator = new ConvexHull (pointArray, convexHullCreator);
				convexHullResult = computator.getConvexHull();
				pointArray = convexHullResult.getCoordinates();
				convexHullPoint = Arrays.asList(pointArray);
				return convexHullPoint;

			}}
		);
		
		
		// Repartition all results onto a local machine
		local = initial.coalesce(1, true);
		convergedResult = local.mapPartitions(new FlatMapFunction<Iterator<Coordinate>, Coordinate>() {
			private static final long serialVersionUID = 1L;

			public Iterable<Coordinate> call(Iterator<Coordinate> CoordinateList) throws Exception {
				Coordinate temp = null;
				Coordinate[] pointArray = null;
				Geometry convexHullResult = null;
				GeometryFactory convexHullCreator = null;
				List<Coordinate> allCoordinates = new ArrayList<Coordinate>();
				List<Coordinate> convexHullPoint = null;
				
				
				// Go through Coordinate in the RDD and add it to a list
				while (CoordinateList.hasNext()) {
					temp = CoordinateList.next();
					allCoordinates.add(temp);
				}
				
				// Return  the result of the local convex hull of each partition
				pointArray = new Coordinate[allCoordinates.size()];
				allCoordinates.toArray(pointArray);
				convexHullCreator = new GeometryFactory();
				ConvexHull computator = new ConvexHull (pointArray, convexHullCreator);
				convexHullResult = computator.getConvexHull();
				pointArray = convexHullResult.getCoordinates();
				convexHullPoint = Arrays.asList(pointArray);
				convexHullPoint = farthest_pair(convexHullPoint);
				
				// Sort the results first by x then y coordinates
				Collections.sort(convexHullPoint, new Comparator<Coordinate>(){
					public int compare(Coordinate a, Coordinate b){
						return compareTo(a, b);
					}
				});
				return convexHullPoint;

			}}
		);
		
		// Convert the results into a RDD
		JavaRDD<String> farthest_pair = convergedResult.map(new Function<Coordinate, String>() {
			private static final long serialVersionUID = 1L;
			public String call (Coordinate s) {
				return (new String(Double.toString(s.x) + "," + Double.toString(s.y)));

			}
		});		
		
		// Save the output to filepath
		farthest_pair.saveAsTextFile(args[1]);
		sc.close();  
	}
}