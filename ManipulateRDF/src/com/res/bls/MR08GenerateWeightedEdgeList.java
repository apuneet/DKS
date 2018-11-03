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

public class MR08GenerateWeightedEdgeList {

	public static class MapSubjects extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, Text> {
		@Override
		public void map(LongWritable inKey, Text inVal,
				OutputCollector<Text, Text> oc, Reporter rep)
				throws IOException {
			// <Subject> GT/RT/ST <SubType>
			// <subject> IDF GT/RT/ST <FromType or R(u)> <in-degree>
			// <Subject> <predicate> <reference> <score> - Forward edge
			// <Subject> AS<ReferredAs> <ReferringSubject> <GT/RT/ST>
			// <TypeofReferringSubject> <score>
			String stInVal = inVal.toString();

			StringTokenizer strTok = new StringTokenizer(stInVal);
			int tokCount = strTok.countTokens();
			if (tokCount < 2) {
				System.out.println(tokCount + ":----->" + stInVal + "<------");
			}
			String stTerm1 = strTok.nextToken();
			String stTerm2 = strTok.nextToken();

			if (stTerm2.startsWith("IDF") || stTerm2.startsWith("GT")
					|| stTerm2.startsWith("RT") || stTerm2.startsWith("ST")) {
				return;
			}
			Text mapKey = null, mapVal = null;

			String stScore = "";

			mapKey = new Text(stTerm1);
			String stTerm3 = strTok.nextToken();
			String stTerm4 = strTok.nextToken();
			if (stTerm2.startsWith("AS")) {
				mapKey = new Text(stTerm1);
				stScore = stInVal.substring(stInVal.lastIndexOf(" ") + 1);
				mapVal = new Text(stTerm3 + " BKD " + stScore);
			} else {
				mapKey = new Text(stTerm1);
				mapVal = new Text(stTerm3 + " FWD " + stTerm4);
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
			while (mappedItr.hasNext()) {
				oc.collect(mappedKey, mappedItr.next());
			}
		}// end of reduce method
	} // end of reduce class
}