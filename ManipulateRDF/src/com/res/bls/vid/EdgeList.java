package com.res.bls.vid;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.lib.MultipleInputs;

import com.res.Constants;

public class EdgeList {

	public static void main(String[] args) throws Exception {

		JobConf conf1 = new JobConf(EdgeList.class);
		conf1.setJobName("Join");

		conf1.setNumReduceTasks(Constants.NUM_REDUCE_TASKS);

		conf1.setOutputKeyClass(Text.class);
		conf1.setOutputValueClass(Text.class);

		conf1.setReducerClass(StatusReducer.class);

		MultipleInputs.addInputPath(conf1, new Path(args[0] + "/part-*"),
				TextInputFormat.class, Mapper1.class);
		MultipleInputs.addInputPath(conf1, new Path(args[1] + "/part-*"),
				TextInputFormat.class, Mapper2.class);

		System.out.println("path:   *************  " + args[1] + "_join1");
		FileOutputFormat.setOutputPath(conf1, new Path(args[1] + "_join1"));

		JobClient.runJob(conf1);
		System.out
				.println("************************Join 1 completed****************************");

		JobConf conf2 = new JobConf(EdgeList.class);
		System.out.println("after 1");
		conf2.setJobName("Join2");
		System.out.println("after 2");

		conf2.setOutputKeyClass(Text.class);
		System.out.println("after 3");
		conf2.setOutputValueClass(Text.class);
		System.out.println("after 4");

		conf2.setReducerClass(StatusReducer2.class);
		System.out.println("after 5");

		MultipleInputs.addInputPath(conf2, new Path(args[0] + "/part-*"),
				TextInputFormat.class, Mapper1_2.class);
		System.out.println("after 6");
		System.out.println("path:   *************  " + args[1]
				+ "_join1/part-*");
		MultipleInputs.addInputPath(conf2, new Path(args[1] + "_join1/part-*"),
				TextInputFormat.class, Mapper2_2.class);

		System.out.println("after 7");
		FileOutputFormat.setOutputPath(conf2, new Path(args[1] + "_join2"));
		System.out.println("after 8");
		JobClient.runJob(conf2);
		System.out
				.println("************************Join 2 completed****************************");

		JobConf conf3 = new JobConf(EdgeList.class);
		conf3.setJobName("Unique");

		conf3.setOutputKeyClass(Text.class);
		conf3.setOutputValueClass(Text.class);

		conf3.setMapperClass(ProjectionMapper.class);
		conf3.setCombinerClass(ProjectionReducer.class);
		conf3.setReducerClass(ProjectionReducer.class);

		conf3.setInputFormat(TextInputFormat.class);
		conf3.setOutputFormat(TextOutputFormat.class);

		FileInputFormat.setInputPaths(conf3,
				new Path(args[1] + "_join2/part-*"));
		FileOutputFormat.setOutputPath(conf3, new Path(args[1] + "_finale"));

		JobClient.runJob(conf3);

	}
}
