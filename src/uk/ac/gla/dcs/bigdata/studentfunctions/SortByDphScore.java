package uk.ac.gla.dcs.bigdata.studentfunctions;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.function.MapGroupsFunction;
import org.apache.spark.broadcast.Broadcast;

import scala.Tuple3;
import uk.ac.gla.dcs.bigdata.providedstructures.DocumentRanking;
import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;
import uk.ac.gla.dcs.bigdata.providedstructures.Query;
import uk.ac.gla.dcs.bigdata.providedstructures.RankedResult;

public class SortByDphScore implements MapGroupsFunction<String, Tuple3<String, NewsArticle, Double>, DocumentRanking> {
	/**
	 *
	 */
	private static final long serialVersionUID = -7963085385885927574L;

	List<Query> allQueries;
	public SortByDphScore() {}
	public SortByDphScore(Broadcast<List<Query>> allQueries) {
		this.allQueries = allQueries.getValue();
	}
	@Override
	public DocumentRanking call(String key, Iterator<Tuple3<String, NewsArticle, Double>> tuples) {
		List<RankedResult> rankedResult = new ArrayList<>();
		List<Tuple3<String, NewsArticle, Double>> sortedTuples = new ArrayList<>();
		while (tuples.hasNext()) {
			sortedTuples.add(tuples.next());
		}
		Collections.sort(sortedTuples, Collections.reverseOrder((t1, t2) -> Double.compare(t1._3(), t2._3())));

		return (new DocumentRanking(getMatchingQuery(this.allQueries,key), rankedResult));
	}

	public Query getMatchingQuery (List<Query> allquery, String key) {
		Query result = null;
		for(Query qItem : allquery) {
			if(qItem.getOriginalQuery().contains(key)) {
				result = qItem;
				break;
			}
		}
		return result;
	}
}