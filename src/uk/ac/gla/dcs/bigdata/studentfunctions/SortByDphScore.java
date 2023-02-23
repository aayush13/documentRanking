package uk.ac.gla.dcs.bigdata.studentfunctions;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.function.FlatMapGroupsFunction;

import scala.Tuple3;
import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;

public class SortByDphScore implements FlatMapGroupsFunction<String, Tuple3<String, NewsArticle, Double>, Tuple3<String, NewsArticle, Double>> {
	/**
	 *
	 */
	private static final long serialVersionUID = -7963085385885927574L;

	@Override
	public Iterator<Tuple3<String, NewsArticle, Double>> call(String key, Iterator<Tuple3<String, NewsArticle, Double>> tuples) {
		List<Tuple3<String, NewsArticle, Double>> rankedResult = new ArrayList<>();
		List<Tuple3<String, NewsArticle, Double>> sortedTuples = new ArrayList<>();
		while (tuples.hasNext()) {
			sortedTuples.add(tuples.next());
		}
		Collections.sort(sortedTuples, Collections.reverseOrder((t1, t2) -> Double.compare(t1._3(), t2._3())));

		return sortedTuples.iterator();
	}
}