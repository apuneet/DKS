package com.res.type;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

import com.res.Constants;

public class MainType2TypeGraphEdges {

	public static void main(String[] args) throws IOException {
		MainType2TypeGraphEdges thisObj = new MainType2TypeGraphEdges();

		if (args.length < 3) {
			System.err
					.println("Usage: Give <outputPath> <List of input paths>");
		}
		String outputPath = args[0];
		String tempPath = args[1];
		String[] inputPaths = new String[args.length - 2];
		for (int i = 0; i < inputPaths.length; i++) {
			inputPaths[i] = args[i + 2];
		}
		thisObj.convertASub2TypeA(inputPaths, tempPath + "A/");

		String[] newInputPaths = new String[inputPaths.length + 1];
		for (int i = 0; i < inputPaths.length; i++) {
			newInputPaths[i] = inputPaths[i];
		}
		newInputPaths[inputPaths.length] = tempPath + "A/";
		thisObj.convertBRef2Type(newInputPaths, tempPath + "B/");

		inputPaths = new String[1];
		inputPaths[0] = tempPath + "B/";
		thisObj.getUniqueEdges(inputPaths, outputPath);
	}

	private void convertASub2TypeA(String inputPaths[], String outputPath)
			throws IOException {
		JobConf conf = new JobConf(com.res.type.MRAConvertSub2Type.class);
		conf.setJobName("A - Convert Subjects to Type");

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);

		conf.setMapperClass(com.res.type.MRAConvertSub2Type.MapSubjects.class);
		conf.setReducerClass(com.res.type.MRAConvertSub2Type.ReduceSub2Type.class);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		for (String nextInputPath : inputPaths) {
			FileInputFormat.addInputPath(conf, new Path(nextInputPath));
		}
		FileOutputFormat.setOutputPath(conf, new Path(outputPath));

		conf.setNumReduceTasks(Constants.NUM_REDUCE_TASKS);

		JobClient.runJob(conf);
		System.out.println("Job has been Submitted...!!!");
	}

	private void convertBRef2Type(String[] inputPaths, String outputPath)
			throws IOException {
		JobConf conf = new JobConf(com.res.type.MRBConvertRef2Type.class);
		conf.setJobName("B - Convert Refs to Type");

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);

		conf.setMapperClass(com.res.type.MRBConvertRef2Type.MapRefs.class);
		conf.setReducerClass(com.res.type.MRBConvertRef2Type.ReduceRef2Type.class);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		for (String nextInputPath : inputPaths) {
			System.out.println("=========Input PATH - " + nextInputPath);
			FileInputFormat.addInputPath(conf, new Path(nextInputPath));
		}
		FileOutputFormat.setOutputPath(conf, new Path(outputPath));

		conf.setNumReduceTasks(Constants.NUM_REDUCE_TASKS);

		JobClient.runJob(conf);
		System.out.println("Job has been Submitted...!!!");
	}

	private void getUniqueEdges(String[] inputPaths, String outputPath)
			throws IOException {
		JobConf conf = new JobConf(com.res.type.MRBConvertRef2Type.class);
		conf.setJobName("C - Get Unique Edges");

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);

		conf.setMapperClass(com.res.type.MRCUniqueClasses.MapAllEdges.class);
		conf.setReducerClass(com.res.type.MRCUniqueClasses.ReduceEdges.class);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		for (String nextInputPath : inputPaths) {
			System.out.println("=========Input PATH - " + nextInputPath);
			FileInputFormat.addInputPath(conf, new Path(nextInputPath));
		}
		FileOutputFormat.setOutputPath(conf, new Path(outputPath));

		conf.setNumReduceTasks(Constants.NUM_REDUCE_TASKS);

		JobClient.runJob(conf);
		System.out.println("Job has been Submitted...!!!");
	}
}
