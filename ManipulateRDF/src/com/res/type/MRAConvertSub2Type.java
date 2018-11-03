package com.res.type;

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

public class MRAConvertSub2Type {

	public static class MapSubjects extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, Text> {
		@Override
		public void map(LongWritable inKey, Text inVal,
				OutputCollector<Text, Text> oc, Reporter rep)
				throws IOException {
			StringTokenizer strTok = new StringTokenizer(inVal.toString());
			String stKey = "";
			String stVal = "";
			if (strTok.countTokens() > 4 || strTok.countTokens() < 3) {
				return;
			}
			String stSubject = strTok.nextToken();
			String stPredicate = strTok.nextToken();
			String stObject = strTok.nextToken();

			if (stObject.startsWith("\"")) {
				return;
			}

			if (stPredicate
					.equalsIgnoreCase("<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>")) {
				stKey = stSubject;
				stVal = "TTTT:" + stObject;
			} else {
				stKey = stSubject;
				stVal = "REF:" + stObject;
			}
			Text mapKey = new Text(stKey);
			Text mapVal = new Text(stVal);
			oc.collect(mapKey, mapVal);
		}
	}

	public static class ReduceSub2Type extends MapReduceBase implements
			Reducer<Text, Text, Text, Text> {
		@Override
		public void reduce(Text mappedKey, Iterator<Text> mappedItr,
				OutputCollector<Text, Text> oc, Reporter rep)
				throws IOException {
			String stType = "";
			String stKey = "", stVal = "";
			ArrayList<String> alRefs = new ArrayList<String>();
			while (mappedItr.hasNext()) {
				String nextVal = mappedItr.next().toString();
				if (nextVal.startsWith("TTTT:")) {
					stType = nextVal;
				} else if (nextVal.startsWith("REF:")) {
					alRefs.add(nextVal);
				}
			}
			if (stType.equals("")) {
				return;
			}
			stKey = stType;
			Text redKey = null, redVal = null;
			for (String ref : alRefs) {
				stVal = ref;
				redKey = new Text(stKey);
				redVal = new Text(stVal);
				oc.collect(redKey, redVal);
			} // end of for loop
		}// end of reduce method
	} // end of reduce class
}
