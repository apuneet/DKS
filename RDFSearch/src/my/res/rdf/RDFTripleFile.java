package my.res.rdf;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.StringTokenizer;

import my.res.err.MyException;

public class RDFTripleFile {

	ArrayList<RDFResource> resources = new ArrayList<RDFResource>();
	String fileNameWPath = null;
	BufferedReader br = null;
	RDFTripleParser rdfParser = new RDFTripleParser();
	RDFTerm[] threeTerms = null;
	boolean hasMore = true;

	public void setRDFFile(String fileNameWPath) {
		this.fileNameWPath = fileNameWPath;
		try {
			br = new BufferedReader(new FileReader(fileNameWPath));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
	}

	public boolean hasMoreResources() {
		return hasMore;
	}

	public RDFResource getNextResource() {
		String currSubject = "", line = "";
		RDFResource rdfResource = new RDFResource();
		boolean resourceLoaded = false;
		String stInDegree = "", stVID = "";
		String remainingStr = null;
		int isVID_inDeg = 0;
		try {
			if (threeTerms != null) {
				if (threeTerms.length == 1) {
					rdfResource.setSubject(threeTerms[0]);
					remainingStr = threeTerms[0].getRemainingString().trim();
					if (remainingStr.startsWith("VID:")) {
						stVID = remainingStr.substring(4);
						isVID_inDeg = 1;
						rdfResource.setVertexID(stVID);
					} else {
						rdfResource.setInDegree(Integer.parseInt(threeTerms[0]
								.getRemainingString().trim()));
						isVID_inDeg = 2;
					}
				} else {
					rdfResource.setSubject(threeTerms[0]);
					rdfResource.addPredicate(threeTerms[1]);
					rdfResource.addObject(threeTerms[2]);
				}
				currSubject = threeTerms[0].getTermText();
			}
			while ((line = br.readLine()) != null) {
				if (line == null || line.equals("")) {
					continue;
				}
				// System.out.println("next Line = " + line);
				StringTokenizer strTok = new StringTokenizer(line);
				int tokCount = strTok.countTokens();
				if (tokCount > 2) {
					threeTerms = rdfParser.getThreeTerms(line);
				} else {
					threeTerms = rdfParser.getOneTerms(line);
					remainingStr = threeTerms[0].getRemainingString();
					if (remainingStr.startsWith("VID:")) {
						stVID = remainingStr.substring(4).trim();
						isVID_inDeg = 1;
					} else {
						stInDegree = remainingStr;
						isVID_inDeg = 2;
					}
				}
				if (threeTerms[0] == null) {
					continue;
				}
				if (rdfResource.getSubject() == null) {
					rdfResource.setSubject(threeTerms[0]);
					currSubject = threeTerms[0].getTermText();
				}
				if (currSubject.equals(threeTerms[0].getTermText())) {
					if (tokCount > 2) {
						rdfResource.addPredicate(threeTerms[1]);
						rdfResource.addObject(threeTerms[2]);
					} else if (isVID_inDeg == 2) {
						rdfResource.setInDegree(Integer.parseInt(stInDegree
								.trim()));
					} else if (isVID_inDeg == 1) {
						rdfResource.setVertexID(stVID);
					}
				} else {
					resourceLoaded = true;
					break;
				}
			}
			if (!resourceLoaded) {
				hasMore = false;
			}
		} catch (IOException e) {
			e.printStackTrace();
		} catch (MyException e) {
			e.printStackTrace();
		}

		return rdfResource;
	}

	public void markEnd() {
		try {
			br.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}