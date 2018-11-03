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

public class MR05CountINofVFromU {

	public static class MapSubjects extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, Text> {
		@Override
		public void map(LongWritable inKey, Text inVal,
				OutputCollector<Text, Text> oc, Reporter rep)
				throws IOException {
			Text mapKey = null, mapVal = null;
			String stVal = "", stInVal = inVal.toString();
			StringTokenizer strTok = new StringTokenizer(stInVal);

			String stSubject = strTok.nextToken();
			String stPredicate = strTok.nextToken();

			if (!stPredicate.startsWith("AS")) {
				mapKey = new Text(stSubject);
				stVal = stPredicate;
				int startPos = stInVal.indexOf("	");
				stVal = stInVal.substring(startPos + 1);
				oc.collect(mapKey, new Text(stVal));
				return;
			}

			String stObject = strTok.nextToken();
			String stTypeType = strTok.nextToken();
			String stObjectType = strTok.nextToken();
			mapKey = new Text(stSubject + " " + stTypeType + " " + stObjectType);
			mapVal = new Text(stPredicate + " " + stObject + " " + stTypeType
					+ " " + stObjectType);
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
			int startPos = stOrigKey.indexOf(" ");
			int INofVFromU = 0;
			if (startPos > 0) {
				String myStKey = stOrigKey.substring(0, startPos);
				while (mappedItr.hasNext()) {
					oc.collect(new Text(myStKey), mappedItr.next());
					INofVFromU++;
				}
				oc.collect(new Text(myStKey), new Text("IDF "
						+ stOrigKey.substring(startPos).trim() + " "
						+ INofVFromU));
			} else {
				while (mappedItr.hasNext()) {
					oc.collect(mappedKey, mappedItr.next());
				}
			}
		}// end of reduce method
	} // end of reduce class
}
