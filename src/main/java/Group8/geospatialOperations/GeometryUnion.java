package Group8.geospatialOperations;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.operation.union.CascadedPolygonUnion;
 
 /**
 * This class parses the raw Coordinates by iterating through
 * the strings in the CSV file line-by-line. Each line in the CSV
 * is defined as a set of co-ordinates(x1,y1,x2,y2) which forms a rectangle.
 * The call function extracts the list of coordinates from the
 * input CSV file and returns a list of type Geometry defined 
 * in the external package called Java Topology Suite[1].
 *
 * @param	String  raw String from the CSV/TXT file.
 * @return	List	returns a list of geometry object 
 * @author	Prashanna Raghavan 
 * @see		<a href="http://www.vividsolutions.com/jts/JTSHome.htm">Java Topology Suite    
 */

class ParseString implements FlatMapFunction<Iterator<String>,Geometry>,Serializable
{
	private static final long serialVersionUID = 1L;

	List<Geometry> rectanglesList ;
	GeometryFactory geometryFactory ;
	String rawCoordinates ;
	String[] coordinates ;
	Geometry rectangle;

	public ParseString()
	{
		this.rectanglesList = new ArrayList<Geometry>();
		this.geometryFactory = new GeometryFactory();
	}
	
	/**
	 * Iterate through each line of the input file.
	 * Parse the coordinates and get the coordinates which forms the rectangle
	 * and input that as an argument to Coordinate[] array. 
	 * The array is then given as argument to the createPolygon function
	 * available in the GeometryFactory class which returns a geometry object.
	 * The geometry is then added to a list of geometry object which is given
	 * as an argument for Local Union  
	 *  
	 */
	
	public Iterable<Geometry> call(Iterator<String> arg0) throws Exception {
			
		while(arg0.hasNext())
		{
			rawCoordinates = arg0.next();
			coordinates = rawCoordinates.split(",");
			
			/*
			 * Assuming the coordinates extracted from the input file
			 * is of format (x1,y1,x2,y2)
			 * Construct a rectangle in the format 
			 * ((x1,y1),(x1,y2),(x2,y2),(x2,y1),(x1,y1))
			 */
			Coordinate[] rectangleCoordinates = new Coordinate[]
					{
							new Coordinate(Double.parseDouble(coordinates[0]),
									Double.parseDouble(coordinates[1])),
							new Coordinate(Double.parseDouble(coordinates[0]),
									Double.parseDouble(coordinates[3])),
							new Coordinate(Double.parseDouble(coordinates[2]),
									Double.parseDouble(coordinates[3])),
							new Coordinate(Double.parseDouble(coordinates[2]),
									Double.parseDouble(coordinates[1])),
							new Coordinate(Double.parseDouble(coordinates[0]),
									Double.parseDouble(coordinates[1]))
					};
			
			rectangle = geometryFactory.createPolygon(rectangleCoordinates);
			rectanglesList.add(rectangle);
		}
		
		return rectanglesList;
	}

}

/**
 * This class gets the input from the ParseString class or 
 * Local Union RDD which is a list of geometry object
 * and returns the union set of a local/global union RDD
 * after performing CascasedPolygonUnion[1].
 * <p>
 * This class implementation uses the CascadedPolygonUnion API available
 * in the Java Topology Suite framework designed by Martin Davis.
 * The algorithm to union the rectangles is based on the algorithm designed 
 * in the paper "CG_Hadoop: Computational Geometry in MapReduce"[3].
 * <p>
 * "The functionality of CascadedPolygonUnion is to union smaller 
 * subsets of the input together which is extracted from a list of polygons, 
 * then union groups of the resulting polygons, 
 * and so on recursively until the final union of all polygons is computed".[2]
 * <p>
 * The algorithm is efficient when there is high degree of overlap
 * between the rectangles and also when there is no overlap between the 
 * rectangles.
 *   
 * @param	List	a list of Geometry object returned by Local Union 
 * @return	List	a list of Geometry object returned by Local/Global Union 
 * @author	Martin Davis/Prashanna Raghavan  
 * @see		<a href="http://tsusiatsoftware.net/jts/javadoc/com/vividsolutions/jts/operation/union/CascadedPolygonUnion.html">CascadedPolygonUnion API
 * @see		<a href="http://lin-ear-th-inking.blogspot.com/2007/11/fast-polygon-merging-in-jts-using.html">Cascaded Polygon Union Algorithm
 * @see		<a href="http://www-users.cs.umn.edu/~mokbel/papers/gis13b.pdf">CG_Hadoop: Computational Geometry in MapReduce		  
 */

class CascadedUnion implements FlatMapFunction<Iterator<Geometry>,Geometry>,Serializable
{
	List<Geometry> localRectangles;
	List<Geometry> localUnionRectangles;
	Geometry localGeometry;
	Collection<Geometry> rectangles ;
	CascadedPolygonUnion cascadedLocalPolyUnion ;
	Geometry localRectanglesUnion ;
	Geometry resultGeometry ;
	int numberofLocalRectangles ;
	int localRectangle;
	
	public CascadedUnion()
	{
		this.localRectangles = new ArrayList<Geometry>();
		this.localUnionRectangles = new ArrayList<Geometry>();
		this.localRectangle=0;
	}
	
	private static final long serialVersionUID = 1L;

	public Iterable<Geometry> call(Iterator<Geometry> arg0) throws Exception 
	{		
		/*
		 * CascadedPolygonUnion takes in Collection of polygon as a parameter.
		 * So get the list of geometry object and type cast them to Collection. 
		 */
		
		while(arg0.hasNext())
		{
			localGeometry = arg0.next();
			localRectangles.add(localGeometry);
		}
		
		rectangles = localRectangles;
		
		/*
		 * Now the CascadedPolygonUnion function combines all the rectangles
		 * and returns back a single geometry.
		 */
		
		cascadedLocalPolyUnion = new CascadedPolygonUnion(rectangles);
		localRectanglesUnion = cascadedLocalPolyUnion.union();		
		
		/*
		 * A list of Geometry should be returned in order to reuse the same class 
		 * for Local/Global Union. The geometry collection is then iterated over 
		 * <getNumGeometries> geometries and each element of the geometry is then
		 * retrieved and added to the list of geometry object. 
		 */
		
		numberofLocalRectangles = localRectanglesUnion.getNumGeometries();
		
		while(localRectangle<numberofLocalRectangles)
		{
			resultGeometry = (Geometry) localRectanglesUnion.getGeometryN(localRectangle);
			localUnionRectangles.add(resultGeometry);
			localRectangle += 1;
		}
		
		return localUnionRectangles;
	}
}

/**
 * This class gets a list of Geometry objects returned
 * by the Global Union of the input rectangles and sorts 
 * the coordinates based on the x-axis. If the x-axis 
 * are same, the list is sorted based on the y-axis.
 * Duplicates are removed from the list if two or more
 * records have same coordinates. A sorted list of string
 * is then returned and stored as a text file using RDD.
 *   
 * @param	List	a list of Geometry object returned by Global Union
 * @return	List	a list of String object which defines a unique set of coordinates
 * @author	Prashanna Raghavan    
 */

class FinalResult implements FlatMapFunction<Iterator<Geometry>,String>,Serializable
{
	private static final long serialVersionUID = 1L;

	static Coordinate[] finalCoordinates;
	static List<Coordinate> listOfFinalCoordinates;
	List <String> coordList ;
	LinkedHashSet<String> uniqueResultSet ;
	
	public FinalResult()
	{
		this.coordList = new ArrayList<String>();
		this.uniqueResultSet = new LinkedHashSet<String>();
	}
	
	public Iterable<String> call(Iterator<Geometry> arg0) throws Exception {
		
		/*
		 * Get the list of geometry object and convert that to 
		 * List of Coordinate object and sort that in an ascending order 
		 */
		
		while(arg0.hasNext())
		{
			finalCoordinates= arg0.next().getCoordinates();
			listOfFinalCoordinates = Arrays.asList(finalCoordinates);
		}
		
		/*
		 * Sort the collection based on the x-coordinate. 
		 * If the x-coordinates are same, sort that based on y-coordinate.
		 * A sorted set of coordinates is then returned and stored in the
		 * same variable.  
		 */
		
		Collections.sort(listOfFinalCoordinates, new Comparator<Coordinate>() {

			public int compare(Coordinate arg0, Coordinate arg1) 
			{
				int result = Double.compare(arg0.x, arg1.x);
		         if(result==0 ) 
		           result = Double.compare(arg0.y, arg1.y);		            
		         return result;
			}
		});
				
		/*
		 * Iterate through the objects and add it to the 
		 * LinkedHashSet. By doing this duplicates are removed
		 * and the order is retained. The final set of coordinates
		 * is then stored in the list.
		 */
		
		for(Coordinate coordinates: listOfFinalCoordinates)
		{
			uniqueResultSet.add(coordinates.x+","+coordinates.y);
		}
		 
		coordList.addAll(uniqueResultSet);
		
		return coordList;
	}
}


/**
 * This class performs the ParseString, Local/Global Union, FinalResult
 * operations and stores the result in a Java Resilient Distributed Dataset.
 * The finalResult is then stored as a text file in Hadoop Distributed 
 * File System which contains a set of coordinates in the format (x1,y1)
 *
 * @param	String  Input file location
 * @param	String  Output file location
 * 
 * @author	Prashanna Raghavan    
 */

public class GeometryUnion implements Serializable {

	private static final long serialVersionUID = 1L;
	static SparkConf conf;
	static JavaSparkContext sc ;
	static JavaRDD<String> input ;
	static JavaRDD<Geometry> inputRectangle ;
	static JavaRDD<Geometry> rectanglesRDD ;
	static JavaRDD<Geometry> localUnion ;
	static JavaRDD<Geometry> localRectangles ;	
	static JavaRDD<Geometry> globalUnion ;
	
	static JavaRDD<String> finalResult;
	
	public GeometryUnion() {
		// TODO Auto-generated constructor stub
		
	}
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		conf = new SparkConf().setAppName("Group8-GeometryUnion");
		sc = new JavaSparkContext(conf);
		
		/*
		 * ParseString
		 * Parse the input file and return a list of Geometry Object
		 * and store the result in a JavaRDD.
		 */
		input = sc.textFile(args[0]);
		inputRectangle = input.mapPartitions(new ParseString());
		
		/*
		 * Local Union
		 * Perform Cascaded union on the list of Geometry objects returned
		 * in the above step and store the result in a JavaRDD.
		 */
		localUnion = inputRectangle.mapPartitions(new CascadedUnion());	
		rectanglesRDD=localUnion.repartition(1);
		
		/*
		 * Global Union
		 * Perform Cascaded union on the list of Geometry objects returned
		 * in the above step and store the result in a JavaRDD.
		 */
		
		globalUnion = rectanglesRDD.mapPartitions(new CascadedUnion());
		finalResult = globalUnion.mapPartitions(new FinalResult());	
		
		/*
		 * Save the result as a text file using JavaRDD.
		 */
		
		finalResult.saveAsTextFile(args[1]);
	}

}