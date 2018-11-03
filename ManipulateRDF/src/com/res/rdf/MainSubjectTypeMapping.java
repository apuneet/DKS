package com.res.rdf;

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

public class MainSubjectTypeMapping {

	public static void main(String[] args) throws IOException {
		MainSubjectTypeMapping thisObj = new MainSubjectTypeMapping();

		if (args.length < 1) {
			System.err
					.println("Usage: argument list -  <outputPath> <List of input paths>");
		}
		String command = args[0];
		String baseOutputPath = args[1];
		String typeCountPath = baseOutputPath + "TypeCounts/";
		String subTypeMappingPath = baseOutputPath + "SubjectTypeMapping/";
		String subjectReferenceListPath = baseOutputPath
				+ "SubjectReferenceList/";
		String[] inputPaths = new String[args.length - 2];
		for (int i = 0; i < inputPaths.length; i++) {
			inputPaths[i] = args[i + 2];
		}

		if (command.equals("A")) {
			thisObj.getSubjectTypeMapping(inputPaths, subTypeMappingPath);
		}
		if (command.equals("B")) {
			thisObj.getTypeCounts(subTypeMappingPath, typeCountPath);
		}
		if (command.equals("C")) {
			thisObj.getSubjectReferrredAs(inputPaths, subjectReferenceListPath);
		}
		if (command.equals("ALL")) {
			thisObj.getSubjectTypeMapping(inputPaths, subTypeMappingPath);
			thisObj.getTypeCounts(subTypeMappingPath, typeCountPath);
		}

	}

	private void getSubjectTypeMapping(String inputPaths[], String outputPath)
			throws IOException {
		JobConf conf = new JobConf(com.res.type.MRAConvertSub2Type.class);
		conf.setJobName("A - Generate Subject Type Mapping");

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);

		conf.setMapperClass(com.res.rdf.MRAConstructResources.MapSubjects.class);

		conf.setReducerClass(com.res.rdf.MRAConstructResources.ReduceResource.class);

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

	private void getTypeCounts(String inputPath, String outputPath)
			throws IOException {
		JobConf conf = new JobConf(com.res.type.MRAConvertSub2Type.class);
		conf.setJobName("B - Generate Type Counts");

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);

		conf.setMapperClass(com.res.rdf.MRBUniqueTypes.MapSubjects.class);
		conf.setReducerClass(com.res.rdf.MRBUniqueTypes.ReduceResource.class);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		FileInputFormat.addInputPath(conf, new Path(inputPath));
		FileOutputFormat.setOutputPath(conf, new Path(outputPath));

		conf.setNumReduceTasks(Constants.NUM_REDUCE_TASKS);

		JobClient.runJob(conf);
		System.out.println("Job has been Submitted...!!!");
	}

	private void getSubjectReferrredAs(String inputPaths[], String outputPath)
			throws IOException {

		System.out.println("outputPath=" + outputPath);
		JobConf conf = new JobConf(com.res.type.MRAConvertSub2Type.class);
		conf.setJobName("C - Subject Referred As");

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);

		conf.setMapperClass(com.res.rdf.MRCReferredAs.MapSubjectsNReferences.class);

		conf.setReducerClass(com.res.rdf.MRCReferredAs.ReduceReference.class);

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
}
