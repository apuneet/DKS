package com.res.bls.vid;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class StatusReducer2 extends MapReduceBase implements
		Reducer<Text, Text, Text, Text> {
	// Variables to aid the join process
	private String status1, status2;
	private int k2;

	public void reduce(Text key, Iterator<Text> values,
			OutputCollector<Text, Text> output, Reporter reporter)
			throws IOException {
		Text temp;
		// String s1_val[]=null;
		// String s2_val[]={};
		List<String> objects = new ArrayList<String>();
		k2 = 0;

		while (values.hasNext()) {
			String currValue = values.next().toString();
			String splitVals[] = currValue.split("~");

			if (splitVals[0].equals("s1")) {
				status1 = splitVals[1].trim();
				// k1++;
			} else if (splitVals[0].equals("s2")) {
				// getting the file2 and using the same to obtain the Message
				objects.add(splitVals[1].trim());
				k2++;
			}

		}
		for (int i = 0; i < k2; i++) {
			if (!objects.isEmpty()) {
				temp = new Text(objects.get(i));
			} else {
				temp = new Text("");
			}

			output.collect(new Text(status1), temp);
		}

	}
}