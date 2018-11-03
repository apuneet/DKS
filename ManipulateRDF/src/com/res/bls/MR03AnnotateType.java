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

public class MR03AnnotateType {

	public static class MapSubjects extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, Text> {
		@Override
		public void map(LongWritable inKey, Text inVal,
				OutputCollector<Text, Text> oc, Reporter rep)
				throws IOException {
			StringTokenizer strTok = new StringTokenizer(inVal.toString());

			String stSubject = strTok.nextToken();
			String stPredicate = strTok.nextToken();
			String stObject = strTok.nextToken();
			if (stPredicate.startsWith("GT")) {
				stSubject = stSubject + "G";
			} else if (stPredicate.startsWith("RT")) {
				stSubject = stSubject + "R";
			} else if (stPredicate.startsWith("ST")) {
				stSubject = stSubject + "S";
			} else {
				stSubject = stSubject + "Z";
			}
			Text mapKey = new Text(stSubject);
			Text mapVal = new Text(stPredicate + " " + stObject);
			oc.collect(mapKey, mapVal);
		}
	}

	public static class ReduceResource extends MapReduceBase implements
			Reducer<Text, Text, Text, Text> {
		@Override
		public void reduce(Text mappedKey, Iterator<Text> mappedItr,
				OutputCollector<Text, Text> oc, Reporter rep)
				throws IOException {
			String stOrigKey = mappedKey.toString();
			stOrigKey = stOrigKey.substring(0, stOrigKey.length() - 1);
			Text myKey = new Text(stOrigKey);
			String nextVal = "";
			boolean isGT = false, isRT = false, isST = false;
			while (mappedItr.hasNext()) {
				nextVal = mappedItr.next().toString();
				if ((nextVal.startsWith("GT") && (isRT || isST))
						|| (nextVal.startsWith("RT") && isST)) {
					throw new IOException("Values not Sorted: isST=" + isST
							+ ", isRT=" + isRT + ", isGT=" + isGT + ", <K,V>="
							+ mappedKey.toString() + ", " + nextVal);
				}
				if (nextVal.startsWith("GT")) {
					isGT = true;
					oc.collect(myKey, new Text(nextVal));
					continue;
				}
				if (nextVal.startsWith("RT")) {
					if (!isGT) {
						isRT = true;
						oc.collect(myKey, new Text(nextVal));
						continue;
					} else {
						continue;
					}
				}
				if (nextVal.startsWith("ST")) {
					if (!isGT && !isRT) {
						isST = true;
						oc.collect(myKey, new Text(nextVal));
						continue;
					} else {
						continue;
					}
				}
				if (isGT || isRT || isST) {
					if (nextVal.startsWith("AS")) {
						continue;
					}
					oc.collect(myKey, new Text(nextVal));
				} else {
					throw new IOException("Values not Sorted: <K,V>="
							+ mappedKey.toString() + ", " + nextVal);
				}
			}
		}// end of reduce method
	} // end of reduce class
}
