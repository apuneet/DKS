package com.res.vid;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class Map_unique extends MapReduceBase implements
		Mapper<LongWritable, Text, Text, Text> {

	public void map(LongWritable key, Text value,
			OutputCollector<Text, Text> output, Reporter reporter)
			throws IOException {

		String line = value.toString();
		StringTokenizer strTok = new StringTokenizer(line);
		int tokCount = strTok.countTokens();
		if (tokCount < 3) {
			return;
		}
		String stSubject = strTok.nextToken();
		String stPredicate = strTok.nextToken();
		String stObject = strTok.nextToken();
		output.collect(new Text(stSubject), new Text(""));
		if (!stObject.startsWith("\"")) {
			output.collect(new Text(stObject), new Text(""));
		}
	}
}