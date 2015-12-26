package Group8.geospatialOperations;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;

import scala.Tuple2;

/**
 * 
 * Author Alekhya Cheruvu
 * Operations SpatialJoinQuery implementation
 * Course: CSE 512
 */


class parseToCSV implements Function<Tuple2<String,String>,String>,Serializable {
	private static final long serialVersionUID = 1L;

public String call(Tuple2<String, String> s) throws Exception {
	return s._1+","+s._2;
}

}

public class SpatialJoinQuery implements Serializable {

	private static final long serialVersionUID = 1L;
	public static SparkConf conf;
	public static JavaSparkContext sc;
	public static void main(String[] args) {
		
		conf = new SparkConf().setAppName("Group8-SpatialJoinQuery");
		sc = new JavaSparkContext(conf);
		String inputfile1 = args[0];
		String inputfile2 = args[1];
		String outputfile = args[2];
		final String name=args[3];
		JavaPairRDD<String,String> result = computeSpatialJoin(inputfile1,inputfile2,name,sc);
		JavaRDD<String> main_result = result.map(new parseToCSV());
		main_result.saveAsTextFile(outputfile);
		sc.close();
	}
	public static JavaPairRDD<String,String> computeSpatialJoin(String if1,String if2, String n,JavaSparkContext sc)
	{
		String bid_file = if2;
		JavaRDD<String> bid_data = sc.textFile(bid_file);
	

		String aid_file = if1;

		JavaRDD<String> aid_data = sc.textFile(aid_file);
		List<String> aid_list = aid_data.collect();
		String[] aid = aid_list.toArray(new String[0]);
		Broadcast <String[]> shared=sc.broadcast(aid);
		final String[] aid_main=shared.value();
	    
		final String name=n;
		
		JavaPairRDD<String,String> result = bid_data.mapToPair(new PairFunction<String, String, String>() {
			private static final long serialVersionUID = 1L;

			public Tuple2<String, String> call(String line) {

				String lines[] = line.split(",");
				List<String> pair = new ArrayList<String>();
				String bid_id = lines[0];
				double bid_x1 = Double.parseDouble(lines[1]);
				double bid_y1 = Double.parseDouble(lines[2]);
				double bid_x2 = Double.parseDouble(lines[3]);
				double bid_y2 = Double.parseDouble(lines[4]);
				String aid_null = "";
				String result = "";
				
				for (String value : aid_main) {
					String str[] = value.split(",");  /* parsing the string */

					String aid_id = str[0];
					double aid_x1 = Double.parseDouble(str[1]);
					double aid_y1 = Double.parseDouble(str[2]);
					double aid_x2 = 0;
					double aid_y2 = 0;
					double aid_x3 = 0;
					double aid_y3 = 0;
					double aid_x4 = 0;
					double aid_y4 = 0;
					double bid_x3 = 0;
					double bid_y3 = 0;
					double bid_x4 = 0;
					double bid_y4 = 0;
					aid_null = aid_id;


					boolean flag = true;

					if (str.length<=3 ||name.equals("point")) {
						flag = false;

					} else {
						aid_x2 = Double.parseDouble(str[3]);
						aid_y2 = Double.parseDouble(str[4]);
					
				  		aid_x3 = aid_x2;    /* getting the rest two points of the rectangle */
				  		aid_y3 = aid_y1;
				  		aid_x4 = aid_x1;
				  		aid_y4 = aid_y2;
				  		
				  		bid_x3 = bid_x2;  
				  		bid_y3 = bid_y1;
				  		bid_x4 = bid_x1;
				  		bid_y4 = bid_y2;
					}

					if (flag) {  /* checking ofr rectnagles */
						if ((Math.max(bid_x2, bid_x1) > aid_x1) && (Math.max(bid_y2, bid_y1)) > aid_y1
								&& (Math.min(bid_x2, bid_x1) < aid_x1) && (aid_y1 > Math.min(bid_y2, bid_y1))) {
							pair.add(aid_id);
						}
						
						else if((Math.max(bid_x2, bid_x1) > aid_x2) && (Math.max(bid_y2, bid_y1)) > aid_y2
								&& (Math.min(bid_x2, bid_x1) < aid_x2) && (aid_y2 > Math.min(bid_y2, bid_y1))) {
							pair.add(aid_id);
						}

					    else if((Math.max(aid_x2, aid_x1) > bid_x2) && (Math.max(aid_y1, aid_y2)) > bid_y2
								&& (Math.min(aid_x2, aid_x1) < bid_x2) && (bid_y2 > Math.min(aid_y2, aid_y1))) {
					    	pair.add(aid_id);
						}
					    else if((Math.max(aid_x2, aid_x1) > bid_x1) && (Math.max(aid_y1, aid_y2)) > bid_y1
								&& (Math.min(aid_x2, aid_x1) < bid_x1) && (bid_y1 > Math.min(aid_y2, aid_y1))) {
					    	pair.add(aid_id);
						}
					    else if ((Math.max(bid_x4, bid_x3) > aid_x3) && (Math.max(bid_y4, bid_y3)) > aid_y3
								&& (Math.min(bid_x4, bid_x3) < aid_x3) && (aid_y3 > Math.min(bid_y4, bid_y3))) {
					    	pair.add(aid_id);
						}
						
						else if((Math.max(bid_x4, bid_x3) > aid_x4) && (Math.max(bid_y4, bid_y3)) > aid_y4
								&& (Math.min(bid_x4, bid_x3) < aid_x4) && (aid_y4 > Math.min(bid_y4, bid_y3))) {
							pair.add(aid_id);
						}

					    else if((Math.max(aid_x4, aid_x3) > bid_x4) && (Math.max(aid_y3, aid_y4)) > bid_y4
								&& (Math.min(aid_x4, aid_x3) < bid_x4) && (bid_y4 > Math.min(aid_y4, aid_y3))) {
					    	pair.add(aid_id);
						}
					    else if((Math.max(aid_x4, aid_x3) > bid_x3) && (Math.max(aid_y3, aid_y4)) > bid_y3
								&& (Math.min(aid_x4, aid_x3) < bid_x3) && (bid_y3 > Math.min(aid_y4, aid_y3))) {
					    	pair.add(aid_id);
						}
					}

					else {   /* checking for point data type */
						if ((Math.max(bid_x2, bid_x1) > aid_x1) && (Math.max(bid_y2, bid_y1)) > aid_y1
								&& (Math.min(bid_x2, bid_x1) < aid_x1) && (aid_y1 > Math.min(bid_y2, bid_y1))) {
							pair.add(aid_id);
						}
					}
					
				}
				result = pair.toString().replaceAll("\\[|\\]","").replaceAll(", ",",");	 
				
				if("".equals(result))
					return new Tuple2<String, String>(aid_null,"null"); /* if no join query rectnagle for aid it returns null */
				else
					return new Tuple2<String,String>(bid_id,result);
			}
		}).repartition(1).sortByKey();
		
		
		return result;
		//main_result.saveAsTextFile(args[2]);   /* stores the result in output file */
	}
}
