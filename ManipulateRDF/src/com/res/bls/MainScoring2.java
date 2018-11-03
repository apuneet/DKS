package com.res.bls;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
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

public class MainScoring2 {

	public static void main(String[] args) throws IOException {
		MainScoring2 thisObj = new MainScoring2();

		if (args.length < 1) {
			System.err
					.println("Usage: argument list -  <outputPath> <List of input paths>");
		}
		String outputPath = args[0];

		thisObj.getMR09(outputPath + "/08_finale/", outputPath + "/09/");
	}

	private void getMR09(String inputPath, String outputPath)
			throws IOException {
		JobConf conf = new JobConf(com.res.type.MRAConvertSub2Type.class);
		conf.setJobName("BLS09 - Generate Edges for Pregel");

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);

		conf.setMapperClass(com.res.bls.MR09MinofBKDScores.MapSubjects.class);
		conf.setReducerClass(com.res.bls.MR09MinofBKDScores.ReduceResource.class);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		FileInputFormat.addInputPath(conf, new Path(inputPath));
		FileOutputFormat.setOutputPath(conf, new Path(outputPath));

		conf.setNumReduceTasks(Constants.NUM_REDUCE_TASKS);

		JobClient.runJob(conf);
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
