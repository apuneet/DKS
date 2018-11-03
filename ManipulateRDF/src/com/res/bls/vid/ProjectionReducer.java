package com.res.bls.vid;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class ProjectionReducer extends MapReduceBase implements
		Reducer<Text, Text, Text, Text> {

	public void reduce(Text key, Iterator<Text> values,
			OutputCollector<Text, Text> output, Reporter reporter)
			throws IOException {
		int tupleCount = 0;
		while (values.hasNext()) {
			tupleCount++;
			values.next();
		}
		if (tupleCount > 1) {
			System.out
					.println("===================================================");
			System.out.println(tupleCount + ":" + key.toString());
			System.out
					.println("===================================================");
		}
		output.collect(key, new Text(""));
	}
}