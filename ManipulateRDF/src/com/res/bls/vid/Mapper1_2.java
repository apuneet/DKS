package com.res.bls.vid;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class Mapper1_2 extends MapReduceBase implements
		Mapper<LongWritable, Text, Text, Text> {

	private String commonkey, status1, fileTag = "s1~";

	public void map(LongWritable key, Text value,
			OutputCollector<Text, Text> output, Reporter reporter)
			throws IOException {

		// taking one line/record at a time and parsing them into key value
		// pairs
		// String line=value.toString();
		long temp = key.get();
		String values[] = value.toString().split("\t");
		commonkey = values[0].trim();
		status1 = values[1].trim();
		// sending the key value pair out of mapper
		output.collect(new Text(commonkey), new Text(fileTag + status1));
	}
}