package uk.ac.gla.dcs.bigdata.studentfunctions;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.broadcast.Broadcast;

import uk.ac.gla.dcs.bigdata.providedstructures.Query;
import uk.ac.gla.dcs.bigdata.studentstructures.ProcessedNewsArticle;

public class QueryFilterFlatMap implements FlatMapFunction<ProcessedNewsArticle,ProcessedNewsArticle>{
	/**
	 *
	 */
	private static final long serialVersionUID = -6818652056479117600L;
	List<Query> queryList;
	public QueryFilterFlatMap() {}
	public QueryFilterFlatMap(Broadcast<List<Query>> queryList){
		this.queryList = queryList.getValue();
	}

	@Override
	public Iterator<ProcessedNewsArticle> call(ProcessedNewsArticle value) throws Exception {
		Map<String, Integer> termCounts = new HashMap<>();
		List<ProcessedNewsArticle> result = new ArrayList<ProcessedNewsArticle>();
		List<String> mergedContent = new ArrayList<String>();
		mergedContent.addAll(value.getTokenisedContent());
		mergedContent.addAll(value.getTokenisedTitle());

		int count = 0;
		for (Query item : queryList) {
			count = 0;
			List<String> queryTokens = item.getQueryTerms();
			if ( mergedContent.containsAll(queryTokens)) {
				String orginalQuery = item.getOriginalQuery();
				value.setMatchingTerm(orginalQuery);
				result.add(value);
				count++;	// count occurrence function needs to be written
			}
			value.setTermCounts(termCounts);
		}
		//		if(value.getMatchingTerm() != null) {
		//			List<ProcessedNewsArticle> result = new ArrayList<ProcessedNewsArticle>(1);
		//			result.add(value);
		//			return result.iterator();
		//		} else {
		//			List<ProcessedNewsArticle> result = new ArrayList<ProcessedNewsArticle>(0);
		//			result.add(value);
		//			return result.iterator();
		//		}
		return result.iterator();
	}
}