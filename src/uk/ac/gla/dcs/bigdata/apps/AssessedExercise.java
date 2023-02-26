package uk.ac.gla.dcs.bigdata.apps;

import java.io.File;
import java.util.List;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.KeyValueGroupedDataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.util.LongAccumulator;

import scala.Tuple3;
import uk.ac.gla.dcs.bigdata.providedfunctions.NewsFormaterMap;
import uk.ac.gla.dcs.bigdata.providedfunctions.QueryFormaterMap;
import uk.ac.gla.dcs.bigdata.providedstructures.DocumentRanking;
import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;
import uk.ac.gla.dcs.bigdata.providedstructures.Query;
import uk.ac.gla.dcs.bigdata.studentfunctions.CalculateDPHScore;
import uk.ac.gla.dcs.bigdata.studentfunctions.GroupDocsByQuery;
import uk.ac.gla.dcs.bigdata.studentfunctions.ProcessedNewsArticlesMap;
import uk.ac.gla.dcs.bigdata.studentfunctions.SortByDphScore;
import uk.ac.gla.dcs.bigdata.studentfunctions.SumTfForCorpus;
import uk.ac.gla.dcs.bigdata.studentfunctions.TermListMap;
import uk.ac.gla.dcs.bigdata.studentfunctions.TokenCounterFlatMap;
import uk.ac.gla.dcs.bigdata.studentstructures.ProcessedNewsArticle;

/**
 * This is the main class where your Spark topology should be specified.
 *
 * By default, running this class will execute the topology defined in the
 * rankDocuments() method in local mode, although this may be overriden by
 * the spark.master environment variable.
 * @author Richard
 *
 */
public class AssessedExercise {


	public static void main(String[] args) {

		File hadoopDIR = new File("resources/hadoop/"); // represent the hadoop directory as a Java file so we can get an absolute path for it
		System.setProperty("hadoop.home.dir", hadoopDIR.getAbsolutePath()); // set the JVM system property so that Spark finds it

		// The code submitted for the assessed exerise may be run in either local or remote modes
		// Configuration of this will be performed based on an environment variable
		String sparkMasterDef = System.getenv("spark.master");
		if (sparkMasterDef==null)
		{
			sparkMasterDef = "local[2]"; // default is local mode with two executors
		}

		String sparkSessionName = "BigDataAE"; // give the session a name

		// Create the Spark Configuration
		SparkConf conf = new SparkConf()
				.setMaster(sparkMasterDef)
				.setAppName(sparkSessionName);


		// Create the spark session
		SparkSession spark = SparkSession
				.builder()
				.config(conf)
				.getOrCreate();


		// Get the location of the input queries
		String queryFile = System.getenv("bigdata.queries");
		if (queryFile==null)
		{
			queryFile = "data/queries.list"; // default is a sample with 3 queries
		}

		// Get the location of the input news articles
		String newsFile = System.getenv("bigdata.news");
		if (newsFile==null)
		{
			newsFile = "data/TREC_Washington_Post_collection.v3.example.json"; // default is a sample of 5000 news articles
		}

		// Call the student's code
		List<DocumentRanking> results = rankDocuments(spark, queryFile, newsFile);

		// Close the spark session
		spark.close();

		// Check if the code returned any results
		if (results==null) {
			System.err.println("Topology return no rankings, student code may not be implemented, skiping final write.");
		} else {

			// We have set of output rankings, lets write to disk

			// Create a new folder
			File outDirectory = new File("results/"+System.currentTimeMillis());
			if (!outDirectory.exists()) {
				outDirectory.mkdir();
			}

			// Write the ranking for each query as a new file
			for (DocumentRanking rankingForQuery : results) {
				rankingForQuery.write(outDirectory.getAbsolutePath());
			}
		}


	}



	public static List<DocumentRanking> rankDocuments(SparkSession spark, String queryFile, String newsFile) {
		// Load queries and news articles
		Dataset<Row> queriesjson = spark.read().text(queryFile);
		Dataset<Row> newsjson = spark.read().text(newsFile); // read in files as string rows, one row per article

		// Perform an initial conversion from Dataset<Row> to Query and NewsArticle Java objects
		Dataset<Query> queries = queriesjson.map(new QueryFormaterMap(), Encoders.bean(Query.class)); // this converts each row into a Query
		Dataset<NewsArticle> news = newsjson.map(new NewsFormaterMap(), Encoders.bean(NewsArticle.class)); // this converts each row into a NewsArticle

		//----------------------------------------------------------------
		// Your Spark Topology should be defined here
		//----------------------------------------------------------------
		//accumulator for sum of document length for all documents
		LongAccumulator docLengthCount = spark.sparkContext().longAccumulator();
		docLengthCount.reset();

		// total documents in the dataset
		long totalDocuments = news.count();

		// Preprocessing the new articles using tokenizer. The resultant is a ProcessedNewsArticlesMap dataset
		Encoder<ProcessedNewsArticle> processedNewsArticleEncoder = Encoders.bean(ProcessedNewsArticle.class);
		ProcessedNewsArticlesMap preProcessing = new ProcessedNewsArticlesMap(docLengthCount);
		Dataset<ProcessedNewsArticle> processedData = news.map(preProcessing, processedNewsArticleEncoder);


		// get list which contains tokens from all queries.
		TermListMap queryToListConvert = new TermListMap();
		Dataset<String> queryL = queries.flatMap(queryToListConvert, Encoders.STRING());
		List<String> allQueryList = queryL.collectAsList();

		//broadcast queries
		Broadcast<List<Query>> allQueries = JavaSparkContext.fromSparkContext(spark.sparkContext()).broadcast(queries.collectAsList());

		// broadcast list of all tokens for all the queries
		Broadcast<List<String>> queryList = JavaSparkContext.fromSparkContext(spark.sparkContext()).broadcast(allQueryList);

		// term counts using flat map and return document matching a query only if it contains any tokens
		TokenCounterFlatMap flatMapFilter = new TokenCounterFlatMap(queryList);
		Dataset<ProcessedNewsArticle> newsArticlesWithDictionary = processedData.flatMap(flatMapFilter, processedNewsArticleEncoder);

		//getVocab using spark
		SumTfForCorpus tfSumReducer = new SumTfForCorpus();
		ProcessedNewsArticle corpusVocabulary = newsArticlesWithDictionary.reduce(tfSumReducer);
		Map<String, Integer> corpusVocab =  corpusVocabulary.getTermCounts();

		// broadcast total documents in corpus
		Broadcast<Long> docCount = JavaSparkContext.fromSparkContext(spark.sparkContext()).broadcast(totalDocuments);
		// broadcast corpus term frequency
		Broadcast<Map<String,Integer>> corpusTf = JavaSparkContext.fromSparkContext(spark.sparkContext()).broadcast(corpusVocab);

		// broadcast average length of doc
		Double avgDocLen = (double) (docLengthCount.value()/totalDocuments);
		Broadcast<Double> avgDocumentLengthinCorpus = JavaSparkContext.fromSparkContext(spark.sparkContext()).broadcast(avgDocLen);

		// calculate DPH score
		Encoder<Tuple3<String,NewsArticle, Double>> matchedDocEncoder = Encoders.tuple(Encoders.STRING(), Encoders.bean(NewsArticle.class),Encoders.DOUBLE());
		CalculateDPHScore getDph = new CalculateDPHScore(allQueries, docCount, corpusTf, avgDocumentLengthinCorpus);
		Dataset<Tuple3<String,NewsArticle, Double>> matchedDocs = newsArticlesWithDictionary.flatMap(getDph, matchedDocEncoder);

		//group queries based on string
		GroupDocsByQuery groupFunc = new GroupDocsByQuery();
		KeyValueGroupedDataset<String, Tuple3<String,NewsArticle, Double>> grpDocs= matchedDocs.groupByKey(groupFunc,Encoders.STRING());

		//reducer for sorting the above results
		Encoder<DocumentRanking> docRankEncoder = Encoders.bean(DocumentRanking.class);
		SortByDphScore sortFunc = new SortByDphScore(allQueries);
		Dataset<DocumentRanking> rankedDocumentResult = grpDocs.mapGroups(sortFunc, docRankEncoder);
		docLengthCount.reset();

		return rankedDocumentResult.collectAsList(); // replace this with the the list of DocumentRanking output by your topology
	}


}
