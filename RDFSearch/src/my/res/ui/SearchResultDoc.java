package my.res.ui;

public class SearchResultDoc {

	int seqNo;
	int docID;
	float searchScore;
	String dpName;
	String fileName;
	String subject;
	String[] predicates;
	String[] objects;
	String inDegree;
	String vertexId;

	public int getPredicateCount() {
		return predicates.length;
	}

	public String getNextObject(int i) {
		return objects[i];
	}

	public String getNextPredicate(int i) {
		return predicates[i];
	}

	public int getSeqNo() {
		return seqNo;
	}

	public void setSeqNo(int seqNo) {
		this.seqNo = seqNo;
	}

	public int getDocID() {
		return docID;
	}

	public void setDocID(int docID) {
		this.docID = docID;
	}

	public float getSearchScore() {
		return searchScore;
	}

	public void setSearchScore(float searchScore) {
		this.searchScore = searchScore;
	}

	public String getDpName() {
		return dpName;
	}

	public void setDpName(String dpName) {
		this.dpName = dpName;
	}

	public String getFileName() {
		return fileName;
	}

	public void setFileName(String fileName) {
		this.fileName = fileName;
	}

	public String getSubject() {
		return subject;
	}

	public void setSubject(String subject) {
		this.subject = subject;
	}

	public String[] getPredicates() {
		return predicates;
	}

	public void setPredicates(String[] predicates) {
		this.predicates = predicates;
	}

	public String[] getObjects() {
		return objects;
	}

	public void setObjects(String[] objects) {
		this.objects = objects;
	}

	public String getInDegree() {
		return inDegree;
	}

	public void setInDegree(String inDegree) {
		this.inDegree = inDegree;
	}

	public String getVertexId() {
		return vertexId;
	}

	public void setVertexId(String vertexId) {
		this.vertexId = vertexId;
	}
}
