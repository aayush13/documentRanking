package uk.ac.gla.dcs.bigdata.studentfunctions;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.broadcast.Broadcast;

import uk.ac.gla.dcs.bigdata.studentstructures.ProcessedNewsArticle;


// This flat map checks if any terms in the broadcasted list are present in the tokenised title or tokenised content of a document.
public class TokenCounterFlatMap implements FlatMapFunction<ProcessedNewsArticle,ProcessedNewsArticle>{
	/**
	 *
	 */
	private static final long serialVersionUID = -6818652056479117600L;
	List<String> queryList;
	public TokenCounterFlatMap() {}
	public TokenCounterFlatMap(Broadcast<List<String>> queryList){
		this.queryList = queryList.getValue();
	}

	@Override
	public Iterator<ProcessedNewsArticle> call(ProcessedNewsArticle value) throws Exception {
		List<ProcessedNewsArticle> result = new ArrayList<ProcessedNewsArticle>();
		List<String> mergedContent = new ArrayList<String>();
		mergedContent.addAll(value.getTokenisedContent());
		mergedContent.addAll(value.getTokenisedTitle());
		Map<String, Integer> terms = new HashMap<String, Integer>();
		for(String token : queryList) {
			int count = Collections.frequency(mergedContent, token);
			if(count > 0) {
				if(terms.containsKey(token)) {
					terms.put(token, terms.get(token)+count);
				} else {
					terms.put(token,count);
				}
			}
		}
		value.setTermCounts(terms);
		if(terms.size()> 0) {
			result.add(value);
		}
		return result.iterator();
	}
}