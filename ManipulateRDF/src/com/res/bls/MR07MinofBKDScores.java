package com.res.bls;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class MR07MinofBKDScores {

	public static class MapSubjects extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, Text> {
		@Override
		public void map(LongWritable inKey, Text inVal,
				OutputCollector<Text, Text> oc, Reporter rep)
				throws IOException {
			// <Subject> GT/RT/ST <SubType>
			// <subject> IDF GT/RT/ST <FromType or R(u)> <in-degree>
			// <Subject> <predicate> <reference> - Forward edge
			// <Subject> AS<ReferredAs> <ReferringSubject> <GT/RT/ST>
			// <TypeofReferringSubject> <score>
			Text mapKey = null, mapVal = null;
			String stInVal = inVal.toString();
			int startPos = stInVal.indexOf("	");
			String stVal = stInVal.substring(startPos + 1);

			StringTokenizer strTok = new StringTokenizer(stInVal);
			int tokCount = strTok.countTokens();
			String stTerm1 = strTok.nextToken();
			String stTerm2 = strTok.nextToken();
			String stTerm3 = strTok.nextToken();
			mapKey = new Text(stTerm1 + "Z");

			if (stTerm2.startsWith("IDF") || stTerm2.startsWith("GT")
					|| stTerm2.startsWith("RT") || stTerm2.startsWith("ST")) {
				oc.collect(mapKey, new Text(stVal));
				return;
			}
			if (tokCount == 4) {
				oc.collect(mapKey, new Text(stVal));
				mapKey = new Text(stTerm1 + " " + stTerm3 + "A");
				mapVal = new Text("FWD");
			} else {
				mapKey = new Text(stTerm1 + " " + stTerm3 + "B");
				mapVal = new Text(stVal);
			}
			oc.collect(mapKey, mapVal);
		}
	}

	public static class ReduceResource extends MapReduceBase implements
			Reducer<Text, Text, Text, Text> {
		@Override
		public void reduce(Text mappedKey, Iterator<Text> mappedItr,
				OutputCollector<Text, Text> oc, Reporter rep)
				throws IOException {
			String stOrigKey = mappedKey.toString(), nextVal = "";
			stOrigKey = stOrigKey.substring(0, stOrigKey.length() - 1);
			String stVal = "", stScore = "";
			int minScore = 1000000000, score = 0;
			int spacePos = stOrigKey.indexOf(" ");
			boolean skip = false;
			if (spacePos > 0) {
				String myStKey = stOrigKey.substring(0, spacePos);
				while (mappedItr.hasNext()) {
					nextVal = mappedItr.next().toString();
					if (nextVal.startsWith("FWD") && score > 0) {
						throw new IOException("Values not Sorted - <K,V>=<"
								+ mappedKey.toString() + "," + nextVal + ">");
					}
					if (nextVal.startsWith("FWD")) {
						skip = true;
						break;
					}
					if (nextVal.startsWith("AS")) {
						stScore = nextVal
								.substring((nextVal.lastIndexOf(" ") + 1));
						score = (new Integer(stScore));
						if (minScore > score) {
							minScore = score;
							stVal = nextVal;
						}
					} else {
						stVal = nextVal;
					}
				}
				if (!skip) {
					oc.collect(new Text(myStKey), new Text(stVal));
				}
			} else {
				while (mappedItr.hasNext()) {
					oc.collect(new Text(stOrigKey), mappedItr.next());
				}
			}
		}// end of reduce method

		private void printKV(String key, String val) {
			if (key.startsWith("<http://www.rdfabout.com/rdf/usgov/sec/id/cik0000000020>")) {
				System.out.println("<" + key + ", " + val + ">");
			}
		}
	} // end of reduce class
}
