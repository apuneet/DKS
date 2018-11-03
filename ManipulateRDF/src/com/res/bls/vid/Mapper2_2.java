package com.res.bls.vid;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class Mapper2_2 extends MapReduceBase implements
		Mapper<LongWritable, Text, Text, Text> {
	// variables to process delivery report
	private String commonkey, status2, fileTag = "s2~";
	private Text word = new Text();

	public void map(LongWritable key, Text value,
			OutputCollector<Text, Text> output, Reporter reporter)
			throws IOException {
		/*
		 * String line = value.toString(); String values[] = line.split(",");
		 * commonkey = values[0].trim(); status2 = values[1].trim();
		 * output.collect(new Text(commonkey), new Text(fileTag + status2));
		 */
		String line = value.toString();

		String values[] = line.split("\t");

		String token2 = values[1].trim();

		output.collect(new Text(token2), new Text(fileTag + line));

	}
}