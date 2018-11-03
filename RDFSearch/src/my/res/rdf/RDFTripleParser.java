package my.res.rdf;

import java.util.StringTokenizer;

public class RDFTripleParser {
	public static void main(String[] args) {
		String nt = "";
		nt = "<http://bnb.data.bl.uk/id/person/WhiteBarbara1952-/birth>"
				+ " <http://purl.org/vocab/bio/0.1/date>"
				+ " \"1952\"^^<http://www.w3.org/2001/XMLSchema#gYear> .";
		nt = "<http://bnb.data.bl.uk/id/person/WhiteBarbara> "
				+ "<http://www.bl.uk/schemas/bibliographic/blterms#hasContributedTo> "
				+ "<http://bnb.data.bl.uk/id/resource/012027042> .";
		nt = "<http://bnb.data.bl.uk/id/person/Zem%40%EF%B8%A0ts%EF%B8%A1ovIl%CA%B9%40%EF%B8%A0ia%EF%B8%A1>"
				+ " <http://www.w3.org/2000/01/rdf-schema#label> "
				+ "\"Zem@\uFE20ts\uFE21ov, Il\u02B9@\uFE20ia\uFE21\" .";
		nt = "<http://bnb.data.bl.uk/id/person/ZemachDorothyE> "
				+ "<http://www.w3.org/2000/01/rdf-schema#label> \"Zemach , Dorothy E.\" .";
		nt = "<http://bnb.data.bl.uk/id/resource/000044704> "
				+ "<http://iflastandards.info/ns/isbd/elements/P1053> \"20 p.\"@en .";
		RDFTripleParser parseRDF = new RDFTripleParser();
		RDFTerm[] rdfTerms = parseRDF.getThreeTerms(nt);
		System.out.println("T1=" + rdfTerms[0].getTermText());
		System.out.println("T2=" + rdfTerms[1].getTermText());
		System.out.println("T3=" + rdfTerms[2].getTermText());
		System.out.println("URI=" + rdfTerms[2].getLiteralURI());
		System.out.println("Type=" + rdfTerms[2].getLiteralType());
		System.out.println("Lang=" + rdfTerms[2].getLiteralLang());
		System.out.println("Re=" + rdfTerms[2].getRemainingString());
	}

	public RDFTerm[] getOneTerms(String rdfString) {
		RDFTerm rdfTerms[] = new RDFTerm[1];
		rdfTerms[0] = getNextTerm(rdfString);
		if (rdfTerms[0] == null) {
			return rdfTerms;
		}
		return rdfTerms;
	}

	public RDFTerm[] getThreeTerms(String rdfString) {
		RDFTerm rdfTerms[] = new RDFTerm[3];
		rdfTerms[0] = getNextTerm(rdfString);
		if (rdfTerms[0] == null) {
			return rdfTerms;
		}
		rdfTerms[1] = getNextTerm(rdfTerms[0].getRemainingString());
		if (rdfTerms[1] == null) {
			return rdfTerms;
		}
		rdfTerms[2] = getNextTerm(rdfTerms[1].getRemainingString());
		return rdfTerms;
	}

	public RDFTerm getNextTerm(String rdfString) {
		if (rdfString == null || rdfString.length() == 0) {
			return null;
		}

		RDFTerm rdfTerm = new RDFTerm();
		if (rdfString.startsWith("_:")) {
			rdfTerm.setTermType(RDFTerm.RDF_BLANK_NODE);
		}
		if (rdfString.startsWith("\"")) {
			rdfTerm.setTermType(RDFTerm.RDF_LITERAL);
		}
		if (rdfString.startsWith("<")) {
			rdfTerm.setTermType(RDFTerm.RDF_URI);
		}
		if (rdfTerm.getTermType() == 0) {
			return null;
		}
		String[] nextStrDetails = getNextString(rdfString,
				rdfTerm.getTermType());

		rdfTerm.setTermText(nextStrDetails[0]);
		rdfTerm.setRemainingString(nextStrDetails[1]);
		rdfTerm.setliteralSuffix(nextStrDetails[2]);
		// System.out.println("Next Term=" + rdfTerm.getStringTerm());
		return rdfTerm;
	}

	private String[] getNextString(String rdfString, int termType) {
		if (termType == 0 || rdfString == null || rdfString.length() == 0) {
			return null;
		}

		String nextString = "";
		StringTokenizer strTok = null;
		String remainingString = "";
		String literalSuffix = "";

		if (termType == RDFTerm.RDF_BLANK_NODE || termType == RDFTerm.RDF_URI) {
			strTok = new StringTokenizer(rdfString);
			nextString = strTok.nextToken();
			while (strTok.hasMoreElements()) {
				remainingString = remainingString + strTok.nextToken() + " ";
			}
		}
		if (termType == RDFTerm.RDF_LITERAL) {
			strTok = new StringTokenizer(rdfString, "\"");
			nextString = strTok.nextToken();
			if (strTok.hasMoreTokens()) {
				literalSuffix = strTok.nextToken();
				strTok = new StringTokenizer(literalSuffix);
				literalSuffix = strTok.nextToken();
			}
		}

		String[] threeParts = new String[3];
		threeParts[0] = nextString;
		threeParts[1] = remainingString;
		threeParts[2] = literalSuffix;
		return threeParts;
	}

	@Deprecated
	private String[] getNextLiteral(String rdfString) {
		String nextString = "";
		StringTokenizer strTok = null;
		String remainingString = "";
		String literalSuffix = "";

		String nextToken = "";
		boolean endFound = false;
		while (strTok.hasMoreElements()) {
			nextToken = strTok.nextToken();
			if (endFound) {
				remainingString = remainingString + nextToken + " ";
				continue;
			}
			int quotePos = nextToken.indexOf("\"");

			if (quotePos == 0) {
				int nextQuotePos = nextToken.substring(1).indexOf("\"");
				if (nextQuotePos != -1) {
					nextString = nextToken.substring(0, nextQuotePos + 2);
					nextToken = nextToken.substring(nextQuotePos + 1);
				} else {
					nextString = nextToken;
					continue;
				}
			}
			if (quotePos == -1 && !endFound) {
				nextString = nextString + nextToken;
			} else if (quotePos != -1 && !endFound) {
				endFound = true;
				if (nextToken.endsWith("\"")) {
					nextString = nextString + nextToken;
				}
				int langPos = nextToken.indexOf("\"@");
				if (langPos != -1) {
					literalSuffix = nextToken.substring(langPos);
				}
				int typePos = nextString.indexOf("\"^^");
				if (typePos != -1) {
					literalSuffix = nextToken.substring(typePos);
				}
			}
		}

		String[] threeParts = new String[3];
		threeParts[0] = nextString;
		threeParts[1] = remainingString;
		threeParts[2] = literalSuffix;
		return threeParts;
	}
}
