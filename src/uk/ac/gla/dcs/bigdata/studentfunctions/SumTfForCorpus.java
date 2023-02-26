package uk.ac.gla.dcs.bigdata.studentfunctions;
import java.util.HashMap;
import java.util.Map;

import org.apache.spark.api.java.function.ReduceFunction;

import uk.ac.gla.dcs.bigdata.studentstructures.ProcessedNewsArticle;



// a reducer that helps to calculate the sum of term frequencies of all the terms across the corpus.
public class SumTfForCorpus implements ReduceFunction<ProcessedNewsArticle>{

	/**
	 *
	 */
	private static final long serialVersionUID = 3145786445802795368L;

	@Override
	public ProcessedNewsArticle call(ProcessedNewsArticle a, ProcessedNewsArticle b) throws Exception {
		Map<String, Integer> v1 = a.getTermCounts();
		Map<String, Integer> v2 = b.getTermCounts();
		Map<String, Integer> result = new HashMap<String, Integer>(v1);
		for (String key : v2.keySet()) {
			result.put(key, result.getOrDefault(key, 0) + v2.get(key));
		}
		return new ProcessedNewsArticle(result);
	}

}
