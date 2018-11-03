package com.res.vid;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class Reduce_unique extends MapReduceBase implements
		Reducer<Text, Text, Text, Text> {
	private JobConf conf;
	private static int token_number = 0;

	@Override
	public void configure(JobConf job) {
		this.conf = job;
	}

	public void reduce(Text key, Iterator<Text> values,
			OutputCollector<Text, Text> output, Reporter reporter)
			throws IOException {
		String temp = conf.get("mapred.tip.id");
		int start = temp.lastIndexOf("_");
		int idx = Integer.parseInt(temp.substring(start + 1));

		token_number++;
		output.collect(key, new Text("VID:" + idx + "." + token_number));
	}
}