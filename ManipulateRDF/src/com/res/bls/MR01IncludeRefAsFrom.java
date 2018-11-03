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

public class MR01IncludeRefAsFrom {

	public static class MapSubjects extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, Text> {
		@Override
		public void map(LongWritable inKey, Text inVal,
				OutputCollector<Text, Text> oc, Reporter rep)
				throws IOException {
			StringTokenizer strTok = new StringTokenizer(inVal.toString());
			if (strTok.countTokens() < 3) {
				return;
			}
			String stSubject = strTok.nextToken();
			String stPredicate = strTok.nextToken();
			String stObject = strTok.nextToken();
			if (stObject.startsWith("\"")) {
				return;
			}
			// if an rdf-subject refers to itself, such edges get rejected.
			if (stSubject.equals(stObject)) {
				return;
			}
			if (stPredicate
					.startsWith("<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>")) {
				stPredicate = "GT";
			}
			Text mapKey = new Text(stSubject);
			Text mapVal = new Text(stPredicate + " " + stObject);
			oc.collect(mapKey, mapVal);

			if (!stPredicate.startsWith("GT")) {
				mapKey = new Text(stObject);
				mapVal = new Text("AS" + stPredicate + " FROMS" + stSubject);
				oc.collect(mapKey, mapVal);
			}
		}
	}

	public static class ReduceResource extends MapReduceBase implements
			Reducer<Text, Text, Text, Text> {
		@Override
		public void reduce(Text mappedKey, Iterator<Text> mappedItr,
				OutputCollector<Text, Text> oc, Reporter rep)
				throws IOException {
			String nextVal = "";
			boolean isRT = false;
			boolean isGT = false;
			while (mappedItr.hasNext()) {
				nextVal = mappedItr.next().toString();
				if (nextVal.startsWith("GT")) {
					isGT = true;
				}

				if (nextVal.startsWith("AS")) {
					isRT = true;
					StringTokenizer strTok = new StringTokenizer(nextVal);
					String refAs = strTok.nextToken().substring(2);
					oc.collect(mappedKey, new Text("RT " + refAs));
				}
				oc.collect(mappedKey, new Text(nextVal));
			}

			if (isRT || isGT) {
				return;
			}
			String stSubject = mappedKey.toString();
			int endPos = stSubject.lastIndexOf("/");
			String stPredicate = "";
			if (endPos == -1) {
				stPredicate = "BLANK-NODE";
			} else {
				stPredicate = stSubject.substring(0, endPos) + ">";
			}
			nextVal = "ST " + stPredicate;
			oc.collect(mappedKey, new Text(nextVal));
		}// end of reduce method
	} // end of reduce class
}
