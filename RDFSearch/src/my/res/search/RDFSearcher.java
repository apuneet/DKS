package my.res.search;

import java.io.File;
import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;

import my.res.Constants;
import my.res.err.MyException;
import my.res.ui.ResultsBean;
import my.res.ui.SearchResultDoc;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryParser.ParseException;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;

public class RDFSearcher {
	IndexSearcher searcher;
	Analyzer analyzer;
	QueryParser parser;
	ScoreDoc[] scoreDocs = null;

	public static void main(String[] args) throws Exception {
		String LOD_Home = System.getProperty("LOD_HOME");
		String DPName = System.getProperty("DPName");
		int isVIDorSUB = 0;
		RDFSearchConfig searchConfig = new RDFSearchConfig(DPName, LOD_Home,
				Constants.resultsPerPage);
		RDFSearcher thisObj = new RDFSearcher(searchConfig);
		if (args[0].startsWith("V")) {
			isVIDorSUB = 1;
		}
		if (args[0].startsWith("S")) {
			isVIDorSUB = 2;
		}
		thisObj.getRDFResource(args[0].substring(1), isVIDorSUB, true);
	}

	public RDFSearcher(RDFSearchConfig searchConfig) {
		Directory indexDir;
		try {
			// System.out.println("Index Path=" + searchConfig.getIndexPath());
			indexDir = FSDirectory.open(new File(searchConfig.getIndexPath()));
			searcher = new IndexSearcher(indexDir);
			analyzer = new StandardAnalyzer(Version.LUCENE_36);
			parser = new QueryParser(Version.LUCENE_36, "Object", analyzer);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public SearchResultDoc searchLiterals(String queryPhrase,
			boolean printOnConsole) throws IOException, MyException {

		Analyzer analyzer = new StandardAnalyzer(Version.LUCENE_36);
		TokenStream tStream = analyzer.tokenStream("Object", new StringReader(
				queryPhrase));
		tStream.reset();
		CharTermAttribute at = tStream.addAttribute(CharTermAttribute.class);
		BooleanQuery bq = new BooleanQuery();
		while (tStream.incrementToken()) {
			Term t = new Term("Subject", at.toString());
			TermQuery termQuery = new TermQuery(t);
			bq.add(termQuery, BooleanClause.Occur.MUST);
			if (printOnConsole) {
				System.out.print("[" + at.toString() + "]");
			}
		}
		TopDocs topDocsResults = searcher.search(bq, 2);
		scoreDocs = topDocsResults.scoreDocs;
		SearchResultDoc sRes = null;

		if (scoreDocs.length == 0) {
			throw new MyException("No Data Found for " + queryPhrase);
		}
		for (int i = 0; i < scoreDocs.length; i++) {
			sRes = generateResultDoc(scoreDocs[i], i + 1);
		}

		if (printOnConsole) {
			System.out.println("");
			printSearchResultDoc(sRes);
		}
		return sRes;
	}

	public SearchResultDoc getRDFResource(String searchString, int isVIDorSUB,
			boolean printOnConsole) throws IOException, MyException {
		System.out.println("searchString=" + searchString + ", isVIDorSUB="
				+ isVIDorSUB);
		Analyzer analyzer = new StandardAnalyzer(Version.LUCENE_36);
		TokenStream tStream = null;

		if (isVIDorSUB == 1) {
			tStream = analyzer.tokenStream("vid",
					new StringReader(searchString));
		} else if (isVIDorSUB == 2) {
			tStream = analyzer.tokenStream("Subject", new StringReader(
					searchString));
		} else {
			throw new MyException("Only Vertex-ID or Subjct can be looked for");
		}

		tStream.reset();
		CharTermAttribute at = tStream.addAttribute(CharTermAttribute.class);
		BooleanQuery bq = new BooleanQuery();
		while (tStream.incrementToken()) {
			Term t = null;
			if (isVIDorSUB == 1) {
				t = new Term("vid", at.toString());
			} else {
				t = new Term("Subject", at.toString());
			}
			TermQuery termQuery = new TermQuery(t);
			bq.add(termQuery, BooleanClause.Occur.MUST);
			if (printOnConsole) {
				System.out.print("[" + at.toString() + "]");
			}
		}
		if (printOnConsole) {
			System.out.println("");
		}
		TopDocs topDocsResults = searcher.search(bq, 2);
		scoreDocs = topDocsResults.scoreDocs;
		int i = 1;
		SearchResultDoc sRes = null;
		System.out.println("scoreDocs.length=" + scoreDocs.length);
		boolean rFound = false;
		for (int j = 0; j < scoreDocs.length; j++) {
			sRes = generateResultDoc(scoreDocs[j], i);
			if (sRes.getSubject().equals(searchString)
					|| sRes.getVertexId().equals(searchString)) {
				rFound = true;
				break;
			}
		}

		if (!rFound && scoreDocs.length != 0) {
			throw new MyException(scoreDocs.length
					+ " RDF resources for the given Subject, but none matched:"
					+ searchString);
		}
		i++;
		if (printOnConsole) {
			System.out.println("");
			printSearchResultDoc(sRes);
		}
		return sRes;
	}

	public ResultsBean getResults(String queryString, ResultsBean resultBean) {
		// Results are fetched in the following line.
		ensureResults(queryString, resultBean);
		// resultBean will now have results.

		int currCacheSize = scoreDocs.length;
		int startCacheIndex = resultBean.getStartIndex() - 1;
		int maxIndex = resultBean.getStartIndex() + resultBean.getResCount()
				- 1;
		ArrayList<SearchResultDoc> alResultDocs = new ArrayList<SearchResultDoc>();
		SearchResultDoc nextDoc = null;
		for (int i = startCacheIndex; i < currCacheSize && i < maxIndex; i++) {
			nextDoc = generateResultDoc(scoreDocs[i], i + 1);
			alResultDocs.add(nextDoc);
		}
		resultBean.setAlResultDocs(alResultDocs);
		return resultBean;
	}

	private int fetchResults(String queryString, int resultCount) {
		Query query;
		TopDocs topDocsResults = null;
		try {
			query = parser.parse(queryString);
			topDocsResults = searcher.search(query, resultCount);
			scoreDocs = topDocsResults.scoreDocs;
		} catch (ParseException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return topDocsResults.totalHits;
	}

	private void ensureResults(String queryString, ResultsBean rBean) {
		int totalHits = 0;
		if (rBean.getCurrPageNo() == 1 && rBean.getStartIndex() == 1) {
			totalHits = fetchResults(queryString, Constants.resultLotSize);
			rBean.setTotalHits(totalHits);
			return;
		}
		int cacheSize = scoreDocs.length;
		int reqdCacheSize = rBean.getStartIndex() + rBean.getResCount() - 1;
		if (cacheSize < reqdCacheSize) {
			int newCacheSize = cacheSize + Constants.resultLotSize;
			totalHits = fetchResults(queryString, newCacheSize);
			rBean.setTotalHits(totalHits);
		}
	}

	private SearchResultDoc generateResultDoc(ScoreDoc scoreDoc, int seqNo) {
		SearchResultDoc sRes = new SearchResultDoc();
		sRes.setSeqNo(seqNo);
		sRes.setDocID(scoreDoc.doc);
		sRes.setSearchScore(scoreDoc.score);
		Document doc = null;
		try {
			doc = searcher.doc(scoreDoc.doc);
		} catch (IOException e) {
			e.printStackTrace();
		}
		sRes.setDpName(doc.get("DPName"));
		sRes.setFileName(doc.get("FileName"));
		sRes.setSubject(doc.get("Subject"));
		sRes.setInDegree(doc.get("InDegree"));
		sRes.setPredicates(doc.getValues("Predicate"));
		sRes.setObjects(doc.getValues("Object"));
		sRes.setVertexId(doc.get("vid"));

		return sRes;
	}

	private void printSearchResultDoc(SearchResultDoc sRes) {
		if (sRes == null) {
			return;
		}
		System.out.println("DocID=\t\t" + sRes.getDocID());
		System.out.println("DpName=\t\t" + sRes.getDpName());
		System.out.println("FileName=\t" + sRes.getFileName());
		System.out.println("SearchScore=\t" + sRes.getSearchScore());
		System.out.println("SeqNo=\t\t" + sRes.getSeqNo());
		System.out.println("Subject=\t" + sRes.getSubject());
		System.out.println("InDegree=\t" + sRes.getInDegree());
		System.out.println("Vertex-ID=\t" + sRes.getVertexId());
		System.out
				.println("==============================================================================");
		for (int j = 0; j < sRes.getPredicateCount(); j++) {
			System.out.println(sRes.getNextPredicate(j) + " -- "
					+ sRes.getNextObject(j));
		}
		System.out
				.println("==============================================================================");

	}
}