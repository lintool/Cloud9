package edu.umd.cloud9.io.benchmark;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.lib.IdentityMapper;
import org.apache.hadoop.mapred.lib.IdentityReducer;

import edu.umd.cloud9.io.PairOfInts;

public class HadoopSortRandomPairsOfInts {

	public static void main(String[] args) throws IOException {
		String inputPath = "random-pairs.seq";
		String outputPath = "random-pairs.sorted";
		int numMapTasks = 1;
		int numReduceTasks = 1;

		JobConf conf = new JobConf(HadoopSortRandomPairsOfInts.class);
		conf.setJobName("SortRandomPairsOfInts");

		conf.setNumMapTasks(numMapTasks);
		conf.setNumReduceTasks(numReduceTasks);

		FileInputFormat.setInputPaths(conf, new Path(inputPath));
		FileOutputFormat.setOutputPath(conf, new Path(outputPath));
		FileOutputFormat.setCompressOutput(conf, false);

		conf.setInputFormat(SequenceFileInputFormat.class);
		conf.setOutputKeyClass(PairOfInts.class);
		conf.setOutputValueClass(IntWritable.class);
		conf.setOutputFormat(TextOutputFormat.class);

		conf.setMapperClass(IdentityMapper.class);
		conf.setCombinerClass(IdentityReducer.class);
		conf.setReducerClass(IdentityReducer.class);

		// Delete the output directory if it exists already
		Path outputDir = new Path(outputPath);
		FileSystem.get(conf).delete(outputDir, true);

		long startTime;
		double duration;

		startTime = System.currentTimeMillis();

		JobClient.runJob(conf);

		duration = (System.currentTimeMillis() - startTime) / 1000.0;
		System.out.println("Job took " + duration + " seconds");

	}
}
