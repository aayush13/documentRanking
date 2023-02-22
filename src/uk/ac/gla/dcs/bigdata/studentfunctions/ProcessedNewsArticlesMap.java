package uk.ac.gla.dcs.bigdata.studentfunctions;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.util.LongAccumulator;

import uk.ac.gla.dcs.bigdata.providedstructures.ContentItem;
import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;
import uk.ac.gla.dcs.bigdata.providedutilities.TextPreProcessor;
import uk.ac.gla.dcs.bigdata.studentstructures.ProcessedNewsArticle;

public class ProcessedNewsArticlesMap implements MapFunction<NewsArticle, ProcessedNewsArticle>{
	/**
	 *
	 */
	private static final long serialVersionUID = 3868880217693153641L;
	LongAccumulator docLengthCount;

	public ProcessedNewsArticlesMap() {}
	public ProcessedNewsArticlesMap(LongAccumulator docLengthCount){
		this.docLengthCount = docLengthCount;
	}

	@Override
	public ProcessedNewsArticle call(NewsArticle value) throws Exception {
		TextPreProcessor processor = new TextPreProcessor();
		String newTitle = value.getTitle();
		List<String> tokens = processor.process(newTitle);
		List<ContentItem> contentIterator = value.getContents();
		List<String> tokenizeContent = new ArrayList<String>();
		int subTypeCounter = 0;
		for(ContentItem item : contentIterator) {
			try {
				String subtype = item.getSubtype();
				if( subtype !=null && subtype.equals("paragraph")) {
					if(subTypeCounter < 5) {
						List<String> contentToken = processor.process(item.getContent());
						tokenizeContent.addAll(contentToken);
						++subTypeCounter;
					}
				} else {
					continue;
				}
			} catch(Exception e) {
				continue;
			}
		}
		ProcessedNewsArticle processedArticle = new ProcessedNewsArticle(value.getId(), value, newTitle, tokens, tokenizeContent, 0.0);
		int docLen = tokenizeContent.size()+ tokens.size();
		docLengthCount.add(docLen);
		processedArticle.setDocumentLength(docLen);
		return processedArticle;
	}
}