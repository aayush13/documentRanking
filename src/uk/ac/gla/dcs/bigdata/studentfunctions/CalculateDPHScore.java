package uk.ac.gla.dcs.bigdata.studentfunctions;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.broadcast.Broadcast;

import scala.Tuple3;
import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;
import uk.ac.gla.dcs.bigdata.providedstructures.Query;
import uk.ac.gla.dcs.bigdata.providedutilities.DPHScorer;
import uk.ac.gla.dcs.bigdata.studentstructures.ProcessedNewsArticle;

public class CalculateDPHScore implements FlatMapFunction<ProcessedNewsArticle, Tuple3<String,NewsArticle,Double>>{

	/**
	 *
	 */
	private static final long serialVersionUID = 8908334526021097768L;
	List<Query> allQueries;
	Long docCount;
	Map<String, Integer> corpusTf;
	Double avgLen;

	public CalculateDPHScore() {}
	public CalculateDPHScore(Broadcast<List<Query>> allQueries, Broadcast<Long> docCount,
			Broadcast<Map<String, Integer>> corpusTf, Broadcast<Double> avgDocumentLengthinCorpus) {
		this.allQueries = allQueries.getValue();
		this.avgLen = avgDocumentLengthinCorpus.getValue();
		this.corpusTf = corpusTf.getValue();
		this.docCount = docCount.getValue();
	}
	@Override
	public Iterator<Tuple3<String, NewsArticle, Double>> call(ProcessedNewsArticle value) throws Exception {
		Map<String, Integer>terms = value.getTermCounts();
		List <Tuple3<String, NewsArticle, Double>> result= new ArrayList<Tuple3<String, NewsArticle, Double>>();
		double score = 0.0;
		double avgScore = 0.0;
		for(Query q : allQueries) {
			List<String> queryTokens = q.getQueryTerms();
			String originalQ = q.getOriginalQuery();
			for (String token : queryTokens) {
				if(terms.containsKey(token)) {
					// calculate dph score
					score += DPHScorer.getDPHScore(terms.get(token).shortValue(),this.corpusTf.get(token), value.getDocumentLength(), this.avgLen, this.docCount);

				} else {
					score += 0.0;
				}
			}
			// calculate average dph score
			avgScore = score/queryTokens.size();
			// create tuple3
			Tuple3<String, NewsArticle, Double> temp = new Tuple3<String, NewsArticle, Double>( originalQ , value.getArticle(), avgScore);
			// add it to a tuple3 list
			if(!Double.isNaN(avgScore) && avgScore >0.0) {
				result.add(temp);
			}
		}
		return result.iterator();
	}

}
