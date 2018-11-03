package com.res.util.MR;

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

public class MRA01CountRDFResources {

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
			Text mapKey = new Text(stSubject);
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
			int nextVal = 0, tuplesPerResource = 0;
			while (mappedItr.hasNext()) {
				nextVal = mappedItr.next().get();
				tuplesPerResource = tuplesPerResource + nextVal;
			}
			oc.collect(mappedKey, new IntWritable(tuplesPerResource));
		}// end of reduce method
	} // end of reduce class
}
