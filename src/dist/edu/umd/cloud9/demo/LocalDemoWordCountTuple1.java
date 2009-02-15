package edu.umd.cloud9.demo;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;

import edu.umd.cloud9.debug.InMemoryOutputCollector;
import edu.umd.cloud9.debug.MapredHarness;
import edu.umd.cloud9.io.Tuple;

public class LocalDemoWordCountTuple1 {

	private LocalDemoWordCountTuple1() {
	}

	public static void main(String[] args) throws IOException {
		String inputPath = "../umd-hadoop-dist/sample-input/bible+shakes.nopunc.tuple1.packed";

		String outputPath = "word-counts-tuple";
		int numMapTasks = 20;
		int numReduceTasks = 20;

		JobConf conf = new JobConf(DemoWordCountTuple1.class);
		conf.setJobName("wordcount");

		conf.setNumMapTasks(numMapTasks);
		conf.setNumReduceTasks(numReduceTasks);

		FileInputFormat.setInputPaths(conf, new Path(inputPath));
		FileOutputFormat.setOutputPath(conf, new Path(outputPath));

		conf.setInputFormat(SequenceFileInputFormat.class);
		conf.setOutputKeyClass(Tuple.class);
		conf.setOutputValueClass(IntWritable.class);
		conf.setOutputFormat(SequenceFileOutputFormat.class);

		conf.setMapperClass(DemoWordCountTuple1.MyMapper.class);
		conf.setCombinerClass(DemoWordCountTuple1.MyReducer.class);
		conf.setReducerClass(DemoWordCountTuple1.MyReducer.class);

		InMemoryOutputCollector collector = MapredHarness.run(conf);
		collector.printAll();
	}

}
