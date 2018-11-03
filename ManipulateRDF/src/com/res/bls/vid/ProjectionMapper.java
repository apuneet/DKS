package com.res.bls.vid;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class ProjectionMapper extends MapReduceBase implements
		Mapper<LongWritable, Text, Text, Text> {

	private String commonkey, status1, fileTag = "s1~";

	public void map(LongWritable key, Text value,
			OutputCollector<Text, Text> output, Reporter reporter)
			throws IOException {

		String values[] = value.toString().split("\t");

		String vertex_ids = values[0].substring(4) + "\t"
				+ values[1].substring(4) + "\t";

		int idx = values[3].indexOf(" ");
		String details = values[3].substring(idx + 1).trim();

		output.collect(new Text(vertex_ids + details), new Text(""));
	}
}