package com.res.type;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class MRBConvertRef2Type {
	public static class MapRefs extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, Text> {
		@Override
		public void map(LongWritable inKey, Text inVal,
				OutputCollector<Text, Text> oc, Reporter rep)
				throws IOException {
			StringTokenizer strTok = new StringTokenizer(inVal.toString());
			String stKey = "";
			String stVal = "";
			int tokCount = strTok.countTokens();
			if (tokCount < 2 || tokCount > 4) {
				return;
			}
			String stTerm1 = strTok.nextToken();
			String stTerm2 = strTok.nextToken();

			// System.out.println("********M2:K,V=" + inKey.toString()
			// + "    ,   " + inVal.toString());

			if (tokCount == 2 && inVal.toString().startsWith("TTTT:")) {
				stKey = stTerm2.substring(4);
				stVal = stTerm1;
			}
			if (tokCount > 2 && !inVal.toString().startsWith("TTTT:")) {
				String stSubject = stTerm1;
				String stPredicate = stTerm2;
				String stObject = strTok.nextToken();

				if (stPredicate
						.equalsIgnoreCase("<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>")) {
					stKey = stSubject;
					stVal = stObject;
				}
			}
			if (!stKey.equals("") && !stVal.equals("")) {
//				System.out.println("*********M:K,V=" + stKey + "     ,    "
//						+ stVal);
				Text mapKey = new Text(stKey);
				Text mapVal = new Text(stVal);
				oc.collect(mapKey, mapVal);
			}
		}
	}

	public static class ReduceRef2Type extends MapReduceBase implements
			Reducer<Text, Text, Text, Text> {
		@Override
		public void reduce(Text mappedKey, Iterator<Text> mappedItr,
				OutputCollector<Text, Text> oc, Reporter rep)
				throws IOException {
			HashSet<String> leftTypes = new HashSet<String>();
			String stRightType = "";
			while (mappedItr.hasNext()) {
				String nextVal = mappedItr.next().toString();
				if (nextVal.startsWith("TTTT:")) {
					leftTypes.add(nextVal);
				} else {
					stRightType = nextVal;
				}
			}
			if (stRightType.equals("")) {
				return;
			}
			Text redKey = null, redVal = null;
			for (String stKey : leftTypes) {
				redKey = new Text(stKey.substring(5));
				redVal = new Text(stRightType);
				oc.collect(redKey, redVal);
			} // end of for loop
		}// end of reduce method
	} // end of reduce class
}
