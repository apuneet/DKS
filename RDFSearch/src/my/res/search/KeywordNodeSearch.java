package my.res.search;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.queryParser.ParseException;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;

import com.iitd.gs.utils.MyDateUtils;

public class KeywordNodeSearch {
	public static void main(String[] args) {

		Date startDate = MyDateUtils.getCurrentDateTime();

		KeywordNodeSearch kns = new KeywordNodeSearch();

		String LOD_HOME = System.getProperty("LOD_HOME");
		String dpName = args[0];
		String outputPath = args[1];
		String perfPath = args[2];
		int inputsBeforeKwds = 3;
		String[] kwds = new String[args.length - inputsBeforeKwds];

		System.out.println(kns.toString() + "==> LOD_HOME=" + LOD_HOME);
		System.out.println(kns.toString() + "==> OutPutPath=" + outputPath);

		for (int i = 0; i < kwds.length; i++) {
			kwds[i] = args[i + inputsBeforeKwds];
			System.out.println(kns.toString() + "==> Keyword=" + kwds[i]);
		}
		String indexPath = LOD_HOME + dpName + "/index/";

		ArrayList<ArrayList<String>> allResults = kns.getAllRDFNodes(indexPath,
				kwds);
		kns.printToFile(allResults, outputPath, kwds);

		Date endDate = MyDateUtils.getCurrentDateTime();
		long miliSecs = MyDateUtils.dateDiff(startDate, endDate);
		String duration = MyDateUtils.simplifyDiff(miliSecs);
		MyDateUtils.appendToFile(perfPath + "KN.txt", duration + "\n");
	}

	private void printToFile(ArrayList<ArrayList<String>> allResults,
			String outputFile, String[] kwds) {
		int i = 0;
		BufferedWriter out = null;
		FileWriter fstream = null;
		ArrayList<String> alkwdRes = null;
		String outputFileName = null;
		try {
			for (String keyword : kwds) {
				alkwdRes = allResults.get(i++);
				outputFileName = outputFile + "/" + keyword;
				fstream = new FileWriter(outputFileName);
				out = new BufferedWriter(fstream);
				for (String sub : alkwdRes) {
					out.write(sub + " ^" + keyword + "\n");
					// out.write("^" + keyword + " " + sub + "\n");
				}
				out.close();
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private ArrayList<ArrayList<String>> getAllRDFNodes(String indexPath,
			String kwds[]) {
		Directory indexDir;
		Query query;
		TopDocs topDocsResults = null;
		IndexSearcher searcher;
		Analyzer analyzer;
		QueryParser parser;
		ScoreDoc[] scoreDocs = null;
		ArrayList<String> alResSubs = null;
		ArrayList<ArrayList<String>> allResults = new ArrayList<ArrayList<String>>();
		int totalHits = 1;
		try {
			indexDir = FSDirectory.open(new File(indexPath));
			searcher = new IndexSearcher(indexDir);
			analyzer = new StandardAnalyzer(Version.LUCENE_36);
			parser = new QueryParser(Version.LUCENE_36, "Object", analyzer);
			for (String keyword : kwds) {
				alResSubs = new ArrayList<String>();
				query = parser.parse(keyword);
				topDocsResults = searcher.search(query, totalHits);
				totalHits = topDocsResults.totalHits;
				topDocsResults = searcher.search(query, totalHits);
				scoreDocs = topDocsResults.scoreDocs;
				System.out.println("For kwyword:" + keyword + ", Total Hits="
						+ totalHits);
				System.out.println("For kwyword:" + keyword
						+ ", Total Results=" + scoreDocs.length);
				Document doc = null;
				for (ScoreDoc sd : scoreDocs) {
					doc = searcher.doc(sd.doc);
					alResSubs.add(doc.get("vid"));
				}
				allResults.add(alResSubs);
			}
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return allResults;
	}
}
