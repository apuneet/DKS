package my.res.search;

import java.io.File;
import java.io.IOException;

import my.res.err.MyException;
import my.res.rdf.RDFResource;
import my.res.rdf.RDFTripleFile;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;

public class RDFIndexer {
	private IndexWriter writer = null;

	public static void main(String[] args) {
		if (args == null || args.length < 1) {
			System.err
					.println("Usage: java my.res.search.RDFIndexer <dataPackage-name>");
			return;
		}
		String dpName = args[0];
		String LOD_Home = System.getProperty("LOD_HOME");
		if (LOD_Home == null || LOD_Home.equals("") || !LOD_Home.endsWith("/")) {
			System.out.println("LOD_HOME = " + LOD_Home);
			System.err
					.println("Set the Env Variable LOD_HOME ending with \"/\"");
			return;
		}

		System.out.println("LOD_HOME = " + LOD_Home);
		System.out.println("Data Package Name=" + dpName);

		String indexPath = LOD_Home + dpName + "/index";
		RDFIndexer indexer = new RDFIndexer();
		indexer.createWriter(dpName, indexPath);

		try {
			indexer.indexDataPackage(indexer.writer, dpName);
			indexer.writer.close();
		} catch (MyException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private void createWriter(String dpName, String indexPath) {
		try {
			Directory dir = FSDirectory.open(new File(indexPath));
			Analyzer analyzer = new StandardAnalyzer(Version.LUCENE_36);
			IndexWriterConfig indConfig = new IndexWriterConfig(
					Version.LUCENE_36, analyzer);
			writer = new IndexWriter(dir, indConfig);

		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	public void indexDataPackage(IndexWriter writer, String dpName)
			throws MyException {
		String LOD_Home = System.getProperty("LOD_HOME");
		String docsPath = LOD_Home + dpName + "/cleanData/";
		String fileNameWPath = null;
		File docDir = new File(docsPath);
		if (!docDir.exists() || !docDir.canRead()) {
			throw new MyException(
					"Document directory '"
							+ docDir.getAbsolutePath()
							+ "' does not exist or is not readable, please check the path");
		}
		try {
			File[] files = docDir.listFiles();
			for (File f : files) {
				if (f.isDirectory() || f.isHidden() || !f.exists()
						|| !f.canRead()) {
					continue;
				}

				fileNameWPath = f.getCanonicalPath();
				String fileName = f.getName();
				if (!fileName.endsWith(".nt")) {
					System.out.println("Skipping File:" + fileName);
					continue;
				}
				indexRDFFile(writer, dpName, fileNameWPath);
			}
			writer.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void indexRDFFile(IndexWriter writer, String dpName,
			String fileNameWPath) throws IOException {
		System.out.println("Indexing started for :" + dpName + ":"
				+ fileNameWPath);
		long startFileIndex = System.currentTimeMillis();

		RDFTripleFile rdfReader = new RDFTripleFile();
		rdfReader.setRDFFile(fileNameWPath);
		int resourceCount = 0;
		while (rdfReader.hasMoreResources()) {
			resourceCount++;
			RDFResource resource = rdfReader.getNextResource();
			Document doc = getDocument4RDFResource(fileNameWPath, dpName,
					resource);
			try {
				writer.addDocument(doc);
				System.out.print("No. of RDF Resources Indexed:"
						+ resourceCount + "\r");
			} catch (IOException e) {
				e.printStackTrace();
			}

		}
		System.out.println("No. of RDF Resources Indexed:" + resourceCount);
		rdfReader.markEnd();
		long endFileIndex = System.currentTimeMillis();
		System.out.println("Indexing done :" + dpName + ":" + fileNameWPath);
		System.out
				.println("=============================================================================");
		System.out.println("Time Taken = " + (endFileIndex - startFileIndex));
		System.out.println("Num of Docs Writtern = " + writer.numDocs());
		System.out.println("resourceCount = " + resourceCount);
		System.out
				.println("=============================================================================");
	}

	public Document getDocument4RDFResource(String fileName, String dpName,
			RDFResource resource) {
		resource.printResource();
		Document doc = new Document();

		Field fFileName = new Field("FileName", fileName, Field.Store.YES,
				Field.Index.ANALYZED);
		doc.add(fFileName);
		Field fDpName = new Field("DPName", dpName, Field.Store.YES,
				Field.Index.ANALYZED);
		doc.add(fDpName);
		Field fInDegree = new Field("InDegree", resource.getInDegree() + "",
				Field.Store.YES, Field.Index.ANALYZED);
		doc.add(fInDegree);
		Field fSubj = new Field("Subject", resource.getSubject().getTermText(),
				Field.Store.YES, Field.Index.ANALYZED);
		doc.add(fSubj);
		Field fVertexID = new Field("vid", resource.getVertexID(),
				Field.Store.YES, Field.Index.NOT_ANALYZED);
		doc.add(fVertexID);
		Field fPred = null;
		Field fObj = null;
		for (int i = 0; i < resource.getPredicates().size(); i++) {
			fPred = new Field("Predicate", resource.getPredicates().get(i)
					.getTermText(), Field.Store.YES, Field.Index.ANALYZED);
			fObj = new Field("Object", resource.getObjects().get(i)
					.getTermText(), Field.Store.YES, Field.Index.ANALYZED);
			doc.add(fPred);
			doc.add(fObj);
		}
		return doc;
	}
}