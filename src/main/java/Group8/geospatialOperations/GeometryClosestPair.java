package Group8.geospatialOperations;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import Group8.geospatialOperations.ClosestPair;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;

/*
*
* GeometryClosestPair.java is a class finds the closest pair of x and y coordinate points
* using Hadoop distributed file system and Apache Spark RDD.
* It uses ClosestPairs and Point2D written by famous algorithm authors
* Robert Sedgewick and Kevin Wayne to perform the divide and conquer algorithm for
* finding closest pairs. Credit for that algorithm goes to them. Garrett is responsible
* for implementation in Hadoop and Apache Spark*

*/
public class GeometryClosestPair implements Serializable {	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
		
	/**
     * Find the closest pair of points given a list of x and y coordinates in a text file
     * using a divide and conquer approach using JavaRDD to distribute the load to partitions
     * @param args[0] is the file of the input file in the format x,y
     * @param args[1] is the file path where the output will be written in sorted form according to x then y coordinate
     * in the format of x,y
     * @return void
     */
	public static void main (String[] args) {
		String logFile  = args[0];
		JavaRDD<String> logData =null;
		JavaRDD<Point2D> parsePoint2Ds = null;
		JavaRDD<Point2D> initial = null;
		JavaRDD<Point2D> local = null;
		JavaRDD<Point2D> convergedResult = null;
		SparkConf conf = new SparkConf().setAppName("Group8-ClosestPair");
	    JavaSparkContext sc = new JavaSparkContext(conf);
	
		// Load the file
		logData = sc.textFile(logFile);
		
		// Start parsing the file into x and y values
		parsePoint2Ds = logData.map(new Function<String,  Point2D>() {
			private static final long serialVersionUID = 1L;
			public Point2D call(String s) {
				String[] Point2DParser = null;
				Point2DParser = s.split(",");
				return new Point2D(Double.parseDouble(Point2DParser[0]), Double.parseDouble(Point2DParser[1]));

			}
		});
		
		// Partition Coordinates on multiple workers and perform local convex hull on each worker
		initial = parsePoint2Ds.mapPartitions(new FlatMapFunction<Iterator<Point2D>, Point2D>() {
			private static final long serialVersionUID = 1L;

			public Iterable<Point2D> call(Iterator<Point2D> Point2DList) throws Exception {
				int i = 0;
				Point2D temp = null;
				Point2D[] pointList = null;
				List<Point2D> allPoint2Ds = new ArrayList<Point2D>(); 
				List<Point2D> closestPair = new ArrayList<Point2D>();
				ClosestPair cp = null;
				
				// Go through Coordinate in the RDD and add it to a list
				while (Point2DList.hasNext()) {
					i++;
					temp = Point2DList.next();
					allPoint2Ds.add(temp);
				}
				
				//return the closest pair of results for each partition
				pointList = new Point2D[i];
				allPoint2Ds.toArray(pointList);
				cp = new ClosestPair(pointList);
				closestPair.add(cp.other());
				closestPair.add(cp.either());
				return closestPair;
			}}
		);
		
		local = initial.coalesce(1, true);
		convergedResult = local.mapPartitions(new FlatMapFunction<Iterator<Point2D>, Point2D>() {
			private static final long serialVersionUID = 1L;

			public Iterable<Point2D> call(Iterator<Point2D> Point2DList) throws Exception {
				int i = 0;
				Point2D temp = null;
				Point2D[] pointList = null;
				List<Point2D> allPoint2Ds = new ArrayList<Point2D>(); 
				List<Point2D> closestPair = new ArrayList<Point2D>();
				ClosestPair cp = null;
				
				// Go through Coordinate in the RDD and add it to a list
				while (Point2DList.hasNext()) {
					i++;
					temp = Point2DList.next();
					allPoint2Ds.add(temp);
				}
				
				//return the closest pair of results for each partition
				pointList = new Point2D[i];
				allPoint2Ds.toArray(pointList);
				cp = new ClosestPair(pointList);
				closestPair.add(cp.other());
				closestPair.add(cp.either());
				
				// Sort the coordinates first by x coordinate then by y coordinate
				Collections.sort(closestPair, new Comparator<Point2D>(){
					public int compare(Point2D a, Point2D b){
						return a.comparison(b);
					}
				});
				return closestPair;
			}}
		);
		
		// Map the result to strings
		JavaRDD<String> closest_pair = convergedResult.map(new Function<Point2D, String>() {
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			public String call(Point2D s) {
				return (new String(Double.toString(s.x()) + "," + Double.toString(s.y())));

			}
		});
		
		// Save the result as a text file
		closest_pair.saveAsTextFile(args[1]);
		sc.close();
	}
}