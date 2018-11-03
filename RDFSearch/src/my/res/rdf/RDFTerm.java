package my.res.rdf;

public class RDFTerm {
	public static final int RDF_URI = 1;
	public static final int RDF_LITERAL = 2;
	public static final int RDF_BLANK_NODE = 3;

	int termType = 0;
	
	String vertexId="";
	
	String termText = "";
	String literalLang = "";
	String literalType = "";
	String literalURI = "";
	String remainingString = "";

	@Override
	public String toString() {
		return termText;
	}

	public void setliteralSuffix(String literalSuffix) {
		if (literalSuffix.startsWith("@")) {
			setLiteralLang(literalSuffix.substring(1));
		}
		if (literalSuffix.startsWith("^^xsd:")) {
			setLiteralType(literalSuffix.substring(2));
		}
		if (literalSuffix.startsWith("^^<")) {
			setLiteralURI(literalSuffix.substring(2));
		}
	}

	public String getRemainingString() {
		return remainingString;
	}

	public void setRemainingString(String remainingString) {
		this.remainingString = remainingString;
	}

	public String getLiteralLang() {
		return literalLang;
	}

	public void setLiteralLang(String literalLang) {
		this.literalLang = literalLang;
	}

	public String getLiteralType() {
		return literalType;
	}

	public void setLiteralType(String literalType) {
		this.literalType = literalType;
	}

	public String getLiteralURI() {
		return literalURI;
	}

	public void setLiteralURI(String literalURI) {
		this.literalURI = literalURI;
	}

	public String getTermText() {
		return termText;
	}

	public void setTermText(String stringTerm) {
		this.termText = stringTerm;
	}

	public int getTermType() {
		return termType;
	}

	public void setTermType(int termType) {
		this.termType = termType;
	}

	
	public String getVertexId() {
		return vertexId;
	}

	public void setVertexId(String vertexId) {
		this.vertexId = vertexId;
	}
	
}
