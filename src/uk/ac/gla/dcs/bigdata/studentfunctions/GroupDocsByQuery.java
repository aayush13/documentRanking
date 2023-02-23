package uk.ac.gla.dcs.bigdata.studentfunctions;

import org.apache.spark.api.java.function.MapFunction;

import scala.Tuple3;
import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;

public class GroupDocsByQuery implements MapFunction<Tuple3<String,NewsArticle, Double>, String>{
	/**
	 *
	 */
	private static final long serialVersionUID = -8453243647628015669L;

	public GroupDocsByQuery() {}

	@Override
	public String call(Tuple3<String, NewsArticle, Double> value) throws Exception {
		// TODO Auto-generated method stub
		return value._1();
	}


}
