package uk.ac.gla.dcs.bigdata.studentfunctions;

import java.util.Iterator;

import org.apache.spark.api.java.function.FlatMapFunction;

import uk.ac.gla.dcs.bigdata.providedstructures.Query;

public class TermListMap implements FlatMapFunction<Query, String>{
	/**
	 *
	 */
	private static final long serialVersionUID = 882090789337866211L;

	public TermListMap() {}

	@Override
	public Iterator<String> call(Query value) throws Exception {
		return value.getQueryTerms().iterator();
	}
}
