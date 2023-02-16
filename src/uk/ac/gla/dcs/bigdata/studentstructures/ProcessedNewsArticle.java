package uk.ac.gla.dcs.bigdata.studentstructures;

import java.io.Serializable;
import java.util.List;

import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;

public class ProcessedNewsArticle implements Serializable {

	private static final long serialVersionUID = 1029529112663235715L;
	String docid;
	NewsArticle article;
	String titleText;
	List<String> tokenisedTitle;
	List<String> tokenisedContent;
	double score;

	public ProcessedNewsArticle() {
		this.score = 0.0;
	}

	public String getDocid() {
		return docid;
	}

	public void setDocid(String docid) {
		this.docid = docid;
	}

	public NewsArticle getArticle() {
		return article;
	}

	public void setArticle(NewsArticle article) {
		this.article = article;
	}

	public String getTitleText() {
		return titleText;
	}

	public void setTitleText(String titleText) {
		this.titleText = titleText;
	}

	public List<String> getTokenisedTitle() {
		return tokenisedTitle;
	}

	public void setTokenisedTitle(List<String> tokenisedTitle) {
		this.tokenisedTitle = tokenisedTitle;
	}

	public List<String> getTokenisedContent() {
		return tokenisedContent;
	}

	public void setTokenisedContent(List<String> tokenisedContent) {
		this.tokenisedContent = tokenisedContent;
	}

	public double getScore() {
		return score;
	}

	public void setScore(double score) {
		this.score = score;
	}
}