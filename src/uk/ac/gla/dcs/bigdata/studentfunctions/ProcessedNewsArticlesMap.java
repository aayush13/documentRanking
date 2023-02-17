package uk.ac.gla.dcs.bigdata.studentfunctions;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.function.MapFunction;

import uk.ac.gla.dcs.bigdata.providedstructures.ContentItem;
import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;
import uk.ac.gla.dcs.bigdata.providedutilities.TextPreProcessor;
import uk.ac.gla.dcs.bigdata.studentstructures.ProcessedNewsArticle;

public class ProcessedNewsArticlesMap implements MapFunction<NewsArticle, ProcessedNewsArticle>{
	/**
	 *
	 */
	private static final long serialVersionUID = 3868880217693153641L;

	@Override
	public ProcessedNewsArticle call(NewsArticle value) throws Exception {
		TextPreProcessor processor = new TextPreProcessor();
		String newTitle = value.getTitle();
		List<String> tokens = processor.process(newTitle);
		List<ContentItem> contentIterator = value.getContents();
		List<String> tokenizeContent = new ArrayList<String>();
		int subTypeCounter = 0;
		for(ContentItem item : contentIterator) {
			if(item.getSubtype() == "paragraph") {
				if(subTypeCounter < 5) {
					List<String> contentToken = processor.process(item.getContent());
					tokenizeContent.addAll(contentToken);
				}
				subTypeCounter++;
			}
		}
		ProcessedNewsArticle processedArticle = new ProcessedNewsArticle(value.getId(), value, newTitle, tokens, tokenizeContent, 0.0);
		return processedArticle;
	}
}