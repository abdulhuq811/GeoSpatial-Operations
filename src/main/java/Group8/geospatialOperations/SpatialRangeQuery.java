package Group8.geospatialOperations;
import Group8.geospatialOperations.Point2;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.io.Serializable;
import java.util.Iterator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;


public class SpatialRangeQuery implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	public static SparkConf conf;
	public static JavaSparkContext sc;
	private static List<Point2> RangePartition;
	public static void main(String[] args) {
		
		/*
		 * 
		 * 
		 * */
		JavaRDD<Point2> parsePoints = null;
		JavaRDD<Point2> initial = null;
	
		conf = new SparkConf().setAppName("Group8-SpatialRangeQuery").setMaster("local");
		
		sc = new JavaSparkContext(conf);
		JavaRDD<String> distData = sc.textFile("arealm.csv");
		JavaRDD<String> queryData = sc.textFile("rectangle.csv");
		String query = queryData.first();
						
		final double windowxmax, windowxmin, windowymax, windowymin;
		String[] querywindow = query.split(",");
		final double windowx1 = Double.parseDouble(querywindow[0]);
		final double windowy1 = Double.parseDouble(querywindow[1]);
		final double windowx2 = Double.parseDouble(querywindow[2]);
		final double windowy2 = Double.parseDouble(querywindow[3]);
		windowxmin=Math.min(windowx1, windowx2); windowxmax=Math.max(windowx1, windowx2);		
		windowymin=Math.min(windowy1, windowy2); windowymax=Math.max(windowy1, windowy2);
		Double[] window = new Double[4];
		window[0]=windowxmin;window[1]=windowymin;window[2]=windowxmax;	window[3]=windowymax;
		
		Broadcast<Double[]> br = sc.broadcast(window);
    	final Double[] broad = br.value();
    	
    	parsePoints = distData.map(new Function<String, Point2>() {
			private static final long serialVersionUID = 1L;

			public Point2 call(String s) {
				String[] points = null;
				points = s.split(",");		
				return new Point2(Double.parseDouble(points[0]), Double.parseDouble(points[1]),Double.parseDouble(points[2]));	
			}
		});		
		initial = parsePoints.mapPartitions(new FlatMapFunction<Iterator<Point2>, Point2>() {
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;
			public List<Point2> call(Iterator<Point2> pointList) throws Exception {
				Point2 temp = null;
				List<Point2> allPoints = new ArrayList<Point2>();

				while (pointList.hasNext()) {
					if(pointList.hasNext()){
					temp = pointList.next();
					allPoints.add(temp);
					}	}
				int iterate1 = 0;	
				int size = allPoints.size();

				double x1 = 0, y1 = 0;
				RangePartition = new ArrayList<Point2>();
				while(iterate1 < size) {
					
					x1 = allPoints.get(iterate1).get_x1();
					y1 = allPoints.get(iterate1).get_y1();
					if (x1 > broad[0] && x1 < broad[2] && y1 > broad[1] && y1 < broad[3]) {
							Point2 point  = new Point2();
							point.set_id(allPoints.get(iterate1).get_id());
							point.set_x1(allPoints.get(iterate1).get_x1());
							point.set_y1(allPoints.get(iterate1).get_y1());
							RangePartition.add(point);
						
					}	iterate1 = iterate1 + 1;
				}
			return RangePartition;
			}
		});		
		
		JavaRDD<Point2> op1= initial.coalesce(1, true);
		
		JavaRDD<Point2> op2 = op1.mapPartitions(new FlatMapFunction<Iterator<Point2>, Point2>() {
			public List<Point2> call(Iterator<Point2> pointList) throws Exception {
				Point2 temp = null;
				List<Point2> allPoints = new ArrayList<Point2>();
				
				while (pointList.hasNext()) {
					if(pointList.hasNext()){
						temp = pointList.next();
						allPoints.add(temp);
					}
				}
				Collections.sort(allPoints, new Comparator<Point2>(){
					public int compare(Point2 p1, Point2 p2){
							return p1.compareTo(p2);
					}
				});
				return allPoints;
			}});
		
		JavaRDD<String> resultString = op2.map(new Function<Point2, String>() {
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			public String call(Point2 s) {
			
				return new String(s.toString());
			}
		});		
    	resultString.saveAsTextFile("rangeoutput");
        sc.close();        
	}
}