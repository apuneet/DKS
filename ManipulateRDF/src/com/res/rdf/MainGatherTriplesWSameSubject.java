package com.res.rdf;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

import com.res.Constants;

public class MainGatherTriplesWSameSubject {

	public static void main(String[] args) throws IOException {
		MainGatherTriplesWSameSubject thisObj = new MainGatherTriplesWSameSubject();

		if (args.length < 1) {
			System.err
					.println("Usage: argument list -  <outputPath> <List of input paths>");
		}
		String outputPath = args[0];
		String[] inputPaths = new String[args.length - 1];
		for (int i = 0; i < inputPaths.length; i++) {
			inputPaths[i] = args[i + 1];
		}

		thisObj.getSubjectTypeMapping(inputPaths, outputPath);
	}
	
	private void getSubjectTypeMapping(String inputPaths[], String outputPath)
			throws IOException {
		JobConf conf = new JobConf(com.res.type.MRAConvertSub2Type.class);
		conf.setJobName("A - Gather RDF Triples with Same Subject Together");

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);

		conf.setMapperClass(com.res.rdf.MRXGatherRDFSubjects.MapSubjects.class);

		conf.setReducerClass(com.res.rdf.MRXGatherRDFSubjects.ReduceResource.class);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		for (String nextInputPath : inputPaths) {
			FileInputFormat.addInputPath(conf, new Path(nextInputPath));
		}
		FileOutputFormat.setOutputPath(conf, new Path(outputPath));

		conf.setNumReduceTasks(Constants.NUM_REDUCE_TASKS);

		JobClient.runJob(conf);
		System.out
				.println("RDF Triples with Same Subject are together now in following Path:");
		System.out.println(outputPath);
		System.out
				.println("====================================================================");
	}
}
