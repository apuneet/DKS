package com.res.bls;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class MR10NodeInDegree {

	public static class MapSubjects extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, IntWritable> {
		@Override
		public void map(LongWritable inKey, Text inVal,
				OutputCollector<Text, IntWritable> oc, Reporter rep)
				throws IOException {
			StringTokenizer strTok = new StringTokenizer(inVal.toString());
			int tokCount = strTok.countTokens();
			if (tokCount < 3) {
				return;
			}
			String stSubject = strTok.nextToken();
			String stPredicate = strTok.nextToken();
			String stObject = strTok.nextToken();
			if (stObject.startsWith("\"")) {
				return;
			}
			if (stPredicate
					.startsWith("<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>")) {
				return;
			}
			Text mapKey = new Text(stObject);
			IntWritable mapVal = new IntWritable(1);
			oc.collect(mapKey, mapVal);
		}
	}

	public static class ReduceResource extends MapReduceBase implements
			Reducer<Text, IntWritable, Text, IntWritable> {
		@Override
		public void reduce(Text mappedKey, Iterator<IntWritable> mappedItr,
				OutputCollector<Text, IntWritable> oc, Reporter rep)
				throws IOException {
			int nextVal = 0, inDegree = 0;
			while (mappedItr.hasNext()) {
				nextVal = mappedItr.next().get();
				inDegree = inDegree + nextVal;
			}
			oc.collect(mappedKey, new IntWritable(inDegree));
		}// end of reduce method
	} // end of reduce class
}
