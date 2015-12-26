package Group8.geospatialOperations;

import java.util.ArrayList;
import java.util.List;
import java.util.*;
import java.io.Serializable;
import java.util.Collections;
import Group8.geospatialOperations.Point;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;

/**
 * @author Mohamed Abdul Huq Ismail
 *
 */

public class GeometryConvexHull implements Serializable {
	private static final long serialVersionUID = 1L;
	public static SparkConf conf;
	public static JavaSparkContext sc;
	
	/*
	 * The credit for the Monotone Chaining Convex Hull Algorithm goes to A.M Andrew. The author of GeometryConvexHull
	 * is responsible only for the implementation of the algorithm in a distributed environment.
	 */
	
	//Check where the Point P2 lies with respect to line (P0-->P1)
	public static double turn(Point P0,Point P1, Point P2)
	{
		return (P1.getX()-P0.getX())*(P2.getY()-P0.getY())-(P2.getX()-P0.getX())*(P1.getY()-P0.getY());
	}
	
	public static List<Point> convexHull(List<Point> xy)
	{
		//Declaration
		List<Point> lowerHull = new ArrayList<Point>();
		List<Point> upperHull = new ArrayList<Point>();
		List<Point> convexHull = new ArrayList<Point>();
		int n,lsize,usize;
		Point temp = null;
		
		//Size of input points
		n = xy.size();
		
		//Sort in ascending order
		Collections.sort(xy, new Comparator<Point>(){
			public int compare(Point p1, Point p2){
				return p1.compareTo(p2);
			}
		});
		
		//If only 3 points are available
		if(n<=3 && n>0)
		{
			for(int i=0;i<n;i++)
			{
				temp = xy.get(i);
				convexHull.add(temp);
			}
		}
		
		//If no points are available
		else if(n <= 0)
			convexHull = null;
		
		//If more than 3 points are available
		else if(n>3)
		{
			//Compute Upper Hull
			for(int i=0;i<n;i++)
			{
				while(upperHull.size() >= 2 && turn(upperHull.get(upperHull.size()-2),upperHull.get(upperHull.size()-1),xy.get(i)) >= 0)
						{
							usize = upperHull.size();
							upperHull.remove(usize-1);
						}
					temp = xy.get(i);
					upperHull.add(temp);
			}
			usize = upperHull.size();
			upperHull.remove(usize-1);
			
			//Compute Lower Hull
			for(int i=n-1;i>=0;i--)
			{
				while(lowerHull.size() >= 2 && turn(lowerHull.get(lowerHull.size()-2),lowerHull.get(lowerHull.size()-1),xy.get(i)) >= 0)
					{
						lsize = lowerHull.size();
						lowerHull.remove(lsize-1);
					}
				temp = xy.get(i);
				lowerHull.add(temp);
			}
			lsize = lowerHull.size();
			lowerHull.remove(lsize-1);
			
			//Join the Upper and Lower Hulls
			for(int i=0;i<upperHull.size();i++)
			{
				temp = upperHull.get(i);
				convexHull.add(temp);
			}
			
			for(int i=0;i<lowerHull.size();i++)
			{
				temp = lowerHull.get(i);
				convexHull.add(temp);
			}
		}
		
		//return the computed result
		return convexHull;
	}
	
	public static void main(String args[])
	{
		//Location of input file 
		String inputfilename = args[0];
		//Location of output file
		String outputfilename = args[1];
		computeConvexHull(inputfilename,outputfilename);
		sc.close();
	}
	
	public static void computeConvexHull(String inputfilename,String outputfilename)
	{
		conf = new SparkConf().setAppName("Group8-GeometryConvexHull");
		sc = new JavaSparkContext(conf);
		JavaRDD<String> lines = sc.textFile(inputfilename);
		
		//Parse String x,y to Point x,y
		JavaRDD<Point> parsePoints = lines.map(new Function<String, Point>(){
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			public Point call(String s){
				String[] parser = null;
				if(s.isEmpty())
					return null;
				else
				{
					parser = s.split(",");
					return new Point(Double.parseDouble(parser[0]),Double.parseDouble(parser[1]));
				}
			}
		});
	
		//Compute local Convex Hull on the workers
		JavaRDD<Point> localConvexHull = parsePoints.mapPartitions(new FlatMapFunction<Iterator<Point>,Point>(){
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			public Iterable<Point> call(Iterator<Point> p) throws Exception {
				Point temp = null;
				List<Point> aP = new ArrayList<Point>();
				while(p.hasNext())
				{
					temp = p.next();
					aP.add(temp);
				}	
				if(aP.isEmpty())
				{
					return null;
				}
				else
				{
					List<Point> result = convexHull(aP);
					return result;
				}
			}
		});

		//Get the local convex hull as one partition
		JavaRDD<Point> convergedLocalConvexHull = localConvexHull.repartition(1);
		
		//Compute the global convex hull in any one of the workers
		JavaRDD<Point> globalConvexHull = convergedLocalConvexHull.mapPartitions(new FlatMapFunction<Iterator<Point>,Point>(){
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			public Iterable<Point> call(Iterator<Point> p) throws Exception {
				Point temp = null;
				List<Point> aP = new ArrayList<Point>();
				while(p.hasNext())
				{
					temp = p.next();
					aP.add(temp);
				}	
				if(aP.isEmpty())
				{
					return null;
				}
				else
				{
					List<Point> result = convexHull(aP);
					//Sort in ascending order
					Collections.sort(result, new Comparator<Point>(){
						public int compare(Point p1, Point p2){
							return p1.compareTo(p2);
						}
					});
					return result;
				}
			}
		});
		
		//Parse the Points into String of x,y
		JavaRDD<String> pointsInString = globalConvexHull.map(new Function<Point,String>(){
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			public String call(Point p){
				String points = null;
				if(p == null)
				{
					return "Null";
				}
				else
				{
					points = p.toString();
					return points;
				}
			}
		});
		
		//Save the file to the hdfs output location
		pointsInString.saveAsTextFile(outputfilename);
	}
}
