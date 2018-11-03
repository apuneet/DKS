package com.res.rdf;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class MRCReferredAs {

	public static class MapSubjectsNReferences extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, Text> {
		@Override
		public void map(LongWritable inKey, Text inVal,
				OutputCollector<Text, Text> oc, Reporter rep)
				throws IOException {
			StringTokenizer strTok = new StringTokenizer(inVal.toString());

			String stSubject = strTok.nextToken();
			String stPredicate = strTok.nextToken();
			String stObject = strTok.nextToken();

			Text mapKey = new Text(stSubject);
			Text mapVal = new Text("MAIN");
			oc.collect(mapKey, mapVal);

			if (!stObject.startsWith("\"")) {
				mapKey = new Text(stObject);
				mapVal = new Text(stPredicate + " " + stSubject);
				oc.collect(mapKey, mapVal);
			}
		}// end of Map method
	} // end of Map Class

	public static class ReduceReference extends MapReduceBase implements
			Reducer<Text, Text, Text, Text> {
		@Override
		public void reduce(Text mappedKey, Iterator<Text> mappedItr,
				OutputCollector<Text, Text> oc, Reporter rep)
				throws IOException {
			String nextValue = "";
			Integer temp = null;
			HashMap<String, Integer> valCount = new HashMap<String, Integer>();
			while (mappedItr.hasNext()) {
				nextValue = mappedItr.next().toString();
				if (nextValue.equals("MAIN")) {
					continue;
				}
				if (valCount.containsKey(nextValue)) {
					temp = valCount.get(nextValue);
					temp = new Integer(temp.intValue() + 1);
					valCount.put(nextValue, temp);
				} else {
					valCount.put(nextValue, new Integer(1));
				}
			}
			Set<String> valSet = valCount.keySet();
			for (String nextRedVal : valSet) {
				oc.collect(mappedKey,
						new Text(nextRedVal + " " + valCount.get(nextRedVal)));
			}
		}// end of reduce method
	} // end of reduce class
}
