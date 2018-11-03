package com.res.vid;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

import com.res.Constants;

public class GenRDFS2VIDMap {

	public static void main(String[] args) throws Exception {
		JobConf conf = new JobConf(com.res.vid.GenRDFS2VIDMap.class);
		conf.setJobName("Unique");
		// System.out.println("*******************ALPHA********2*****************************");
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);

		conf.setNumReduceTasks(Constants.NUM_REDUCE_TASKS);

		conf.setMapperClass(com.res.vid.Map_unique.class);
		conf.setCombinerClass(com.res.vid.Reduce_unique.class);
		conf.setReducerClass(com.res.vid.Reduce_unique.class);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		// conf.setNumReduceTasks(4);
		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));

		JobClient.runJob(conf);
		System.out.println("Output in Path = " + args[1]);
		System.out
				.println("************************Unique KeyWord completed****************************");

	}
}
