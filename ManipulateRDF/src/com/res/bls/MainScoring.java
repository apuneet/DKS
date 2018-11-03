package com.res.bls;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.lib.HashPartitioner;

import com.res.Constants;

public class MainScoring {

	public static void main(String[] args) throws IOException {
		MainScoring thisObj = new MainScoring();

		if (args.length < 1) {
			System.err
					.println("Usage: argument list -  <outputPath> <List of input paths>");
		}
		String outputPath = args[0];
		String[] inputPaths = new String[args.length - 1];
		for (int i = 0; i < inputPaths.length; i++) {
			inputPaths[i] = args[i + 1];
		}

		thisObj.getMR01(inputPaths, outputPath + "/01/");
		thisObj.getMR02(outputPath + "/01/", outputPath + "/02/");
		thisObj.getMR03(outputPath + "/02/", outputPath + "/03/");
		thisObj.getMR04(outputPath + "/03/", outputPath + "/04/");
		thisObj.getMR05(outputPath + "/04/", outputPath + "/05/");
		thisObj.getMR06(outputPath + "/05/", outputPath + "/06/");
		thisObj.getMR07(outputPath + "/06/", outputPath + "/07/");
		thisObj.getMR08(outputPath + "/07/", outputPath + "/08/");
		thisObj.getMR10(inputPaths, outputPath + "/10/");
	}

	private void getMR01(String inputPaths[], String outputPath)
			throws IOException {
		JobConf conf = new JobConf(com.res.bls.MR01IncludeRefAsFrom.class);
		conf.setJobName("BLS01 - Include GTs, RTs, STs for every rdf-resource, Include reversed reference triples");

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);

		conf.setMapperClass(com.res.bls.MR01IncludeRefAsFrom.MapSubjects.class);
		conf.setReducerClass(com.res.bls.MR01IncludeRefAsFrom.ReduceResource.class);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		for (String nextInputPath : inputPaths) {
			FileInputFormat.addInputPath(conf, new Path(nextInputPath));
		}
		FileOutputFormat.setOutputPath(conf, new Path(outputPath));

		conf.setNumReduceTasks(Constants.NUM_REDUCE_TASKS);
		JobClient.runJob(conf);
		System.out
				.println("RDF Triples with Same Subject are together and have following types of triples:");
		System.out.println("<Subject Predicate Object> Forward Edges");
		System.out
				.println("<Subject AS<ReferredAs> FROMS<ReferredFrom> - Reversed Edge");
		System.out
				.println("<Subject GT SubType> - one rdf-resouce can have more than one GT, this is against given #type");
		System.out
				.println("<Subject RT SubType> - one tuple of this kind for every edge.");
		System.out
				.println("<Subject ST SubType> - one tuple of this kind for every rdf-resource that is not "
						+ "referred and does not have a type defined.");

		System.out.println("Output-Path=" + outputPath);
		System.out
				.println("====================================================================");
	}

	private void getMR02(String inputPath, String outputPath)
			throws IOException {
		JobConf conf = new JobConf(com.res.bls.MR02UniqueRT.MapSubjects.class);
		conf.setJobName("BLS02 - Get Unique RT per Subject");

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);

		conf.setMapperClass(com.res.bls.MR02UniqueRT.MapSubjects.class);

		conf.setReducerClass(com.res.bls.MR02UniqueRT.ReduceResource.class);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		FileInputFormat.addInputPath(conf, new Path(inputPath));
		FileOutputFormat.setOutputPath(conf, new Path(outputPath));

		conf.setNumReduceTasks(Constants.NUM_REDUCE_TASKS);

		JobClient.runJob(conf);
		System.out.println("Every RT is mentioned once for that Subject");
		System.out.println("Output-Path=" + outputPath);
		System.out
				.println("====================================================================");
	}

	private void getMR03(String inputPath, String outputPath)
			throws IOException {
		JobConf conf = new JobConf(
				com.res.bls.MR03AnnotateType.MapSubjects.class);
		conf.setJobName("BLS03 - Annotate Type for every RDF-Resource");

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);

		conf.setOutputValueGroupingComparator(SortReducerByValuesValueGroupingComparator.class);
		conf.setPartitionerClass(SortReducerByValuesPartitioner.class);

		conf.setMapperClass(com.res.bls.MR03AnnotateType.MapSubjects.class);
		conf.setReducerClass(com.res.bls.MR03AnnotateType.ReduceResource.class);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		FileInputFormat.addInputPath(conf, new Path(inputPath));
		FileOutputFormat.setOutputPath(conf, new Path(outputPath));

		conf.setNumReduceTasks(Constants.NUM_REDUCE_TASKS);

		JobClient.runJob(conf);
		System.out
				.println("This job removed all RTs if the rdf-resource has GT");
		System.out.println("Every RDF-Resource has Type marked now, "
				+ "which will be GTs or RTs or ST");
		System.out.println("Note : It does not contain any reversed Edges.");

		System.out.println("Output-Path=" + outputPath);
		System.out
				.println("====================================================================");
	}

	private void getMR04(String inputPath, String outputPath)
			throws IOException {
		JobConf conf = new JobConf(
				com.res.bls.MR04IncludeBkwdEdgesWithType.MapSubjects.class);
		conf.setJobName("BLS04 - Annotate Type for every RDF-Resource reference");

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);

		conf.setOutputValueGroupingComparator(SortReducerByValuesValueGroupingComparator.class);
		conf.setPartitionerClass(SortReducerByValuesPartitioner.class);

		conf.setMapperClass(com.res.bls.MR04IncludeBkwdEdgesWithType.MapSubjects.class);
		conf.setReducerClass(com.res.bls.MR04IncludeBkwdEdgesWithType.ReduceResource.class);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		FileInputFormat.addInputPath(conf, new Path(inputPath));
		FileOutputFormat.setOutputPath(conf, new Path(outputPath));

		conf.setNumReduceTasks(Constants.NUM_REDUCE_TASKS);

		JobClient.runJob(conf);
		System.out.println("This job adds reverse edges in following format.");
		System.out
				.println("<Subject> AS<ReferredAs> <ReferringSubject> <GT/RT/ST> <TypeofReferringSubject>");
		System.out.println("Output-Path=" + outputPath);
		System.out
				.println("====================================================================");
	}

	private void getMR05(String inputPath, String outputPath)
			throws IOException {
		JobConf conf = new JobConf(
				com.res.bls.MR05CountINofVFromU.MapSubjects.class);
		conf.setJobName("BLS05 - Count InDegree of v from R(u)");

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);

		conf.setMapperClass(com.res.bls.MR05CountINofVFromU.MapSubjects.class);
		conf.setReducerClass(com.res.bls.MR05CountINofVFromU.ReduceResource.class);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		FileInputFormat.addInputPath(conf, new Path(inputPath));
		FileOutputFormat.setOutputPath(conf, new Path(outputPath));

		conf.setNumReduceTasks(Constants.NUM_REDUCE_TASKS);

		JobClient.runJob(conf);
		System.out
				.println("This job Counts In-degree of v from R(u) and includes following types of tuples");
		System.out
				.println("<Subject> IDF <GT/RT/ST> <R(u)> <In-degree of v from R(u)>");
		System.out.println("Output-Path=" + outputPath);
		System.out
				.println("====================================================================");
	}

	private void getMR06(String inputPath, String outputPath)
			throws IOException {
		JobConf conf = new JobConf(
				com.res.bls.MR06GenerateWtPossibilities.MapSubjects.class);
		conf.setJobName("BLS06 - Generate bkwd edge weight possibilities");

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);

		conf.setOutputValueGroupingComparator(SortReducerByValuesValueGroupingComparator.class);
		conf.setPartitionerClass(SortReducerByValuesPartitioner.class);

		conf.setMapperClass(com.res.bls.MR06GenerateWtPossibilities.MapSubjects.class);
		conf.setReducerClass(com.res.bls.MR06GenerateWtPossibilities.ReduceResource.class);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		FileInputFormat.addInputPath(conf, new Path(inputPath));
		FileOutputFormat.setOutputPath(conf, new Path(outputPath));

		conf.setNumReduceTasks(Constants.NUM_REDUCE_TASKS);

		JobClient.runJob(conf);
		System.out
				.println("Attach backward edge weight for every possibility.");
		System.out
				.println("<Subject> AS<ReferredAs> <ReferringSubject> <GT/RT/ST> <TypeofReferringSubject> <edgeScore>");
		System.out.println("Output-Path=" + outputPath);
		System.out
				.println("====================================================================");
	}

	private void getMR07(String inputPath, String outputPath)
			throws IOException {
		JobConf conf = new JobConf(
				com.res.bls.MR07MinofBKDScores.MapSubjects.class);
		conf.setJobName("BLS07 - Retain BKD edges with min weight.");

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);

		conf.setOutputValueGroupingComparator(SortReducerByValuesValueGroupingComparator.class);
		conf.setPartitionerClass(SortReducerByValuesPartitioner.class);

		conf.setMapperClass(com.res.bls.MR07MinofBKDScores.MapSubjects.class);
		conf.setReducerClass(com.res.bls.MR07MinofBKDScores.ReduceResource.class);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		FileInputFormat.addInputPath(conf, new Path(inputPath));
		FileOutputFormat.setOutputPath(conf, new Path(outputPath));

		conf.setNumReduceTasks(Constants.NUM_REDUCE_TASKS);

		JobClient.runJob(conf);
		System.out
				.println("Reverse Edges with min wt are retained others get rejected.");
		System.out.println("Output-Path=" + outputPath);
		System.out
				.println("====================================================================");
	}

	private void getMR08(String inputPath, String outputPath)
			throws IOException {
		JobConf conf = new JobConf(
				com.res.bls.MR08GenerateWeightedEdgeList.MapSubjects.class);
		conf.setJobName("BLS08 - Generate Weighted Edge-List");

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);

		conf.setMapperClass(com.res.bls.MR08GenerateWeightedEdgeList.MapSubjects.class);
		conf.setReducerClass(com.res.bls.MR08GenerateWeightedEdgeList.ReduceResource.class);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		FileInputFormat.addInputPath(conf, new Path(inputPath));
		FileOutputFormat.setOutputPath(conf, new Path(outputPath));

		conf.setNumReduceTasks(Constants.NUM_REDUCE_TASKS);

		JobClient.runJob(conf);
		System.out.println("Generates weighted Edge-List in following format");
		System.out.println("<Subject> <Object> <BKD/FWD> <score> ");
		System.out.println("Output-Path=" + outputPath);
		System.out
				.println("====================================================================");
	}

	private void getMR10(String[] inputPaths, String outputPath)
			throws IOException {
		JobConf conf = new JobConf(com.res.type.MRAConvertSub2Type.class);
		conf.setJobName("BLS10 - Node In-Degree");

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);

		conf.setMapperClass(com.res.bls.MR10NodeInDegree.MapSubjects.class);
		conf.setReducerClass(com.res.bls.MR10NodeInDegree.ReduceResource.class);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		for (String nextInputPath : inputPaths) {
			FileInputFormat.addInputPath(conf, new Path(nextInputPath));
		}
		FileOutputFormat.setOutputPath(conf, new Path(outputPath));

		conf.setNumReduceTasks(Constants.NUM_REDUCE_TASKS);

		JobClient.runJob(conf);
		System.out.println("Generates weighted Edge-List in following format");
		System.out.println("<Subject> <Object> <BKD/FWD> <score> ");
		System.out.println("Output-Path=" + outputPath);
		System.out
				.println("====================================================================");
	}

	public static final class SortReducerByValuesValueGroupingComparator
			implements RawComparator<Text> {
		@Override
		public int compare(byte[] text1, int arg1, int arg2, byte[] text2,
				int arg4, int arg5) {
			return new Character((char) text1[0]).compareTo((char) text2[0]);
		}

		@Override
		public int compare(Text key1, Text key2) {
			String stK1 = key1.toString().substring(0,
					key1.toString().length() - 1);
			String stK2 = key2.toString().substring(0,
					key2.toString().length() - 1);
			int val = compare(stK1.getBytes(), 0, stK1.length(),
					stK2.getBytes(), 0, stK2.length());
			val = stK1.compareTo(stK2);
			return val;
		}
	}

	public static final class SortReducerByValuesPartitioner extends
			HashPartitioner<Text, Text> {
		@Override
		public int getPartition(Text key, Text value, int numPartitions) {
			return super
					.getPartition(
							new Text(key.toString().substring(0,
									key.toString().length() - 1)), value,
							numPartitions);
		}
	}

}
