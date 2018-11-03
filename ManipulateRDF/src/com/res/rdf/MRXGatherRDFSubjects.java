package com.res.rdf;

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

public class MRXGatherRDFSubjects {

	public static class MapSubjects extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, Text> {
		@Override
		public void map(LongWritable inKey, Text inVal,
				OutputCollector<Text, Text> oc, Reporter rep)
				throws IOException {
			StringTokenizer strTok = new StringTokenizer(inVal.toString());
			if (strTok.countTokens() < 2) {
				return;
			}

			String stSubject = strTok.nextToken();
			String stRestofLine = "";

			while (strTok.hasMoreTokens()) {
				stRestofLine += strTok.nextToken() + " ";
			}

			Text mapKey = new Text(stSubject);
			Text mapVal = new Text(stRestofLine);
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
			while (mappedItr.hasNext()) {
				nextVal = mappedItr.next().toString();
				oc.collect(mappedKey, new Text(nextVal));
			}

		}// end of reduce method
	} // end of reduce class
}
