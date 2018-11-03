package com.res.bls;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class MR04IncludeBkwdEdgesWithType {

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

			if (stPredicate.startsWith("GT") || stPredicate.startsWith("RT")
					|| stPredicate.startsWith("ST")) {
				stSubject = stSubject + "A";
			} else {
				stSubject = stSubject + "B";
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

			String nextVal = "", stPredicate = "", stObject = "", stKey = "", stValue = "";
			boolean isTypeOver = false;
			ArrayList<String> alTypes = new ArrayList<String>();
			while (mappedItr.hasNext()) {
				nextVal = mappedItr.next().toString();
				if (nextVal.startsWith("GT") || nextVal.startsWith("RT")
						|| nextVal.startsWith("ST")) {
					if (isTypeOver) {
						throw new IOException("Values not Sorted: <K,V>="
								+ mappedKey.toString() + ", " + nextVal);
					}
					alTypes.add(nextVal);
					oc.collect(myKey, new Text(nextVal));
					continue;
				}
				isTypeOver = true;
				StringTokenizer strTok = new StringTokenizer(nextVal);
				stPredicate = strTok.nextToken();
				stObject = strTok.nextToken();
				oc.collect(myKey, new Text(nextVal + " 1"));

				stKey = stObject;
				for (String nextType : alTypes) {
					stValue = "AS" + stPredicate + " " + stOrigKey;
					stValue = stValue + " " + nextType;
					oc.collect(new Text(stKey), new Text(stValue));
				}

			}
		}// end of reduce method
	} // end of reduce class
}
