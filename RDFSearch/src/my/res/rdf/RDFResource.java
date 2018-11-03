package my.res.rdf;

import java.util.ArrayList;

import my.res.err.MyException;

public class RDFResource {

	RDFTerm subject = null;
	ArrayList<RDFTerm> predicates = new ArrayList<RDFTerm>();
	ArrayList<RDFTerm> objects = new ArrayList<RDFTerm>();
	int inDegree = 0;
	String vertexID = "";

	public void printResource() {

		// System.out
		// .println("=================================  RDF Resource  =======================================================");
		// System.out.println("Subject=" + subject);
		// System.out
		// .println("========================================================================================================");
		// for (int i = 0; i < predicates.size(); i++) {
		// System.out.println(predicates.get(i) + " ----- " + objects.get(i));
		// }
		// System.out
		// .println("=======================================  END  ==========================================================");
	}

	public void addPredicate(RDFTerm predicate) {
		predicates.add(predicate);
	}

	public void addObject(RDFTerm object) throws MyException {
		if (subject == null) {
			throw new MyException("No subject specified for the RDF Resource");
		}
		if (objects.size() == predicates.size()) {
			throw new MyException("Add the predicate before adding object");
		}
		objects.add(object);
	}

	public RDFTerm getSubject() {
		return subject;
	}

	public void setSubject(RDFTerm subject) {
		this.subject = subject;
	}

	public ArrayList<RDFTerm> getPredicates() {
		return predicates;
	}

	public ArrayList<RDFTerm> getObjects() {
		return objects;
	}

	public int getInDegree() {
		return inDegree;
	}

	public void setInDegree(int inDegree) {
		this.inDegree = inDegree;
	}

	public String getVertexID() {
		return vertexID;
	}

	public void setVertexID(String vertexID) {
		this.vertexID = vertexID;
	}

}
