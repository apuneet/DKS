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

public class MR06GenerateWtPossibilities {

	public static class MapSubjects extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, Text> {
		@Override
		public void map(LongWritable inKey, Text inVal,
				OutputCollector<Text, Text> oc, Reporter rep)
				throws IOException {
			// <Subject> GT/RT/ST <SubType>
			// <Subject> <predicate> <reference> - Forward edge
			// <subject> IDF GT/RT/ST <FromType or R(u)> <in-degree>
			// <Subject> AS<ReferredAs> <ReferringSubject> <GT/RT/ST>
			// <TypeofReferringSubject> 
			Text mapKey = null, mapVal = null;
			String stInVal = inVal.toString();
			int startPos = stInVal.indexOf("	");
			String stVal = stInVal.substring(startPos + 1);

			StringTokenizer strTok = new StringTokenizer(stInVal);

			String stTerm1 = strTok.nextToken();
			String stTerm2 = strTok.nextToken();

			// <Subject> GT/RT/ST <SubType>
			// <Subject> <predicate> <reference> - Forward edge
			if (!stTerm2.startsWith("AS") && !stTerm2.startsWith("IDF")) {
				mapKey = new Text(stTerm1 + "A");
				oc.collect(mapKey, new Text(stVal));
				return;
			}
			String stTerm3 = strTok.nextToken();
			String stTerm4 = strTok.nextToken();
			String stTerm5 = strTok.nextToken();
			mapVal = new Text(stVal);
			boolean startsWithIDF = false;
			if (stVal.startsWith("IDF")) {
				// 1<subject> 2IDF 3<GT/RT/ST> 4<FromType or R(u)> 5<in-degree>
				mapKey = new Text(stTerm1 + " " + stTerm4 + "A");
				startsWithIDF = true;
			} else {
				// 1<Subject> 2AS<ReferredAs> 3<ReferringSubject> 4<GT/RT/ST>
				// 5<TypeofReferringSubject> 6<InDegree>
				mapKey = new Text(stTerm1 + " " + stTerm5 + "B");
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
			String stVal = "";

			int spacePos = stOrigKey.indexOf(" ");
			if (spacePos > 0) {
				String myStKey = stOrigKey.substring(0, spacePos);
				String INofVFromU = "";
				while (mappedItr.hasNext()) {
					nextVal = mappedItr.next().toString();
					if (nextVal.startsWith("IDF")) {
						stVal = nextVal;
						INofVFromU = (nextVal.substring(nextVal
								.lastIndexOf(" ") + 1));
					}
					if (INofVFromU.length() == 0 && nextVal.startsWith("AS")) {
						throw new IOException("Values not Sorted <K,V>="
								+ mappedKey.toString() + ", " + nextVal);
					}
					if (nextVal.startsWith("AS")) {
						stVal = nextVal + " " + INofVFromU;
					}
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
