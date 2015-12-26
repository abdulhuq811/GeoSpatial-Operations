/**
*
*author_name = Mohamed Abdul Huq Ismail
*
*/
import java.io.Serializable;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;

import scala.Tuple2;

import Group8.geospatialOperations.SpatialJoinQuery;

class countID implements Function<Tuple2<String,String>,String>,Serializable {
	private static final long serialVersionUID = 1L;
	
public String call(Tuple2<String, String> s) throws Exception {
	int size;
	if(s._2.isEmpty())
	{
		size = 0;
	}
	else
	{
		String[] temp = s._2.split(",");
		size = temp.length;
	}
	return s._1+","+size;
}

}

public class aggregation implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	public static SparkConf conf;
	public static JavaSparkContext sc;
	
	public static void main(String[] args)
	{
		conf = new SparkConf().setAppName("Group8-Spatial Aggregation");
		sc = new JavaSparkContext(conf);
		String inputfile1 = args[0];
		String inputfile2 = args[1];
		String outputfile = args[2];
		final String pr = args[3];
		JavaRDD<String> result = computeSpatialAggregation(inputfile1,inputfile2,pr,sc);
		result.saveAsTextFile(outputfile);
	}
	public static JavaRDD<String> computeSpatialAggregation(String if1, String if2, String pr,JavaSparkContext sc)
	{
		JavaPairRDD<String,String> QueryPointPair = SpatialJoinQuery.computeSpatialJoin(if1, if2, pr,sc);
		
		JavaRDD<String> result = QueryPointPair.map(new countID()); 
		return result;
	}
}
