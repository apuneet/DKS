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

public class MR02UniqueRT {

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
			Text mapKey = null, mapVal = null;
			if (stPredicate.equals("RT")) {
				mapKey = new Text(stSubject + " " + stPredicate + " "
						+ stObject);
				mapVal = new Text("A");
			} else {
				mapKey = new Text(stSubject);
				mapVal = new Text(stPredicate + " " + stObject);
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
			String nextVal = "";
			boolean isRT = false;
			while (mappedItr.hasNext()) {
				nextVal = mappedItr.next().toString();
				if (nextVal.equals("A")) {
					isRT = true;
				} else {
					oc.collect(mappedKey, new Text(nextVal));
				}
			}
			if (isRT) {
				oc.collect(mappedKey, new Text(""));
			}
		}// end of reduce method
	} // end of reduce class
}
