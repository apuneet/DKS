package com.res.type;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class MRCUniqueClasses {
	public static class MapAllEdges extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, IntWritable> {
		@Override
		public void map(LongWritable inKey, Text inVal,
				OutputCollector<Text, IntWritable> oc, Reporter arg3)
				throws IOException {
			String stKey = inVal.toString();
			IntWritable opVal = new IntWritable(1);
			oc.collect(new Text(stKey), opVal);
		}
	}

	public static class ReduceEdges extends MapReduceBase implements
			Reducer<Text, IntWritable, Text, IntWritable> {
		@Override
		public void reduce(Text inKey, Iterator<IntWritable> inVal,
				OutputCollector<Text, IntWritable> oc, Reporter rep)
				throws IOException {
			String stKey = inKey.toString();
			int count = 0;
			while (inVal.hasNext()) {
				count = count + inVal.next().get();
			}
			Text opKey = new Text(stKey);
			IntWritable opVal = new IntWritable(count);
			oc.collect(opKey, opVal);
		}
	}
}
