package my.res.ui;

import java.util.ArrayList;

public class ResultsBean {

	String inputQuery;

	ArrayList<SearchResultDoc> alResultDocs;
	int startIndex;
	int currPageNo;
	int resCount;
	int totalHits;

	public String getInputQuery() {
		return inputQuery;
	}

	public void setInputQuery(String inputQuery) {
		this.inputQuery = inputQuery;
	}

	public ArrayList<SearchResultDoc> getAlResultDocs() {
		return alResultDocs;
	}

	public void setAlResultDocs(ArrayList<SearchResultDoc> alResultDocs) {
		this.alResultDocs = alResultDocs;
	}

	public int getTotalHits() {
		return totalHits;
	}

	public void setTotalHits(int totalHits) {
		this.totalHits = totalHits;
	}

	public int getStartIndex() {
		return startIndex;
	}

	public void setStartIndex(int startIndex) {
		this.startIndex = startIndex;
	}

	public int getCurrPageNo() {
		return currPageNo;
	}

	public void setCurrPageNo(int currPageNo) {
		this.currPageNo = currPageNo;
	}

	public int getResCount() {
		return resCount;
	}

	public void setResCount(int resCount) {
		this.resCount = resCount;
	}
}
