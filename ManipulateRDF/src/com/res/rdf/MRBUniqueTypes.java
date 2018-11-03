package com.res.rdf;

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

public class MRBUniqueTypes {

	public static class MapSubjects extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, IntWritable> {
		@Override
		public void map(LongWritable inKey, Text inVal,
				OutputCollector<Text, IntWritable> oc, Reporter rep)
				throws IOException {
			StringTokenizer strTok = new StringTokenizer(inVal.toString());

			String stSubject = strTok.nextToken();
			String stType = strTok.nextToken();

			Text mapKey = new Text(stType);
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
			System.out.println(mappedKey.toString());
			int sum = 0;
			while (mappedItr.hasNext()) {
				mappedItr.next();
				sum++;
			}
			oc.collect(mappedKey, new IntWritable(sum));
		}// end of reduce method
	} // end of reduce class
}
