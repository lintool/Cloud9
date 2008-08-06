package edu.umd.cloud9.demo;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;

import edu.umd.cloud9.debug.InMemoryOutputCollector;
import edu.umd.cloud9.debug.MapredHarness;
import edu.umd.cloud9.io.Tuple;

public class LocalDemoWordCountTuple {

	public static void main(String[] args) throws IOException {
		String inPath = "../umd-hadoop-dist/sample-input/bible+shakes.nopunc.packed";

		String outputPath = "word-counts-tuple";
		int numMapTasks = 20;
		int numReduceTasks = 20;

		JobConf conf = new JobConf(DemoWordCountTuple.class);
		conf.setJobName("wordcount");

		conf.setNumMapTasks(numMapTasks);
		conf.setNumReduceTasks(numReduceTasks);

		conf.setInputPath(new Path(inPath));
		conf.setInputFormat(SequenceFileInputFormat.class);

		conf.setOutputPath(new Path(outputPath));
		conf.setOutputKeyClass(Tuple.class);
		conf.setOutputValueClass(IntWritable.class);
		conf.setOutputFormat(SequenceFileOutputFormat.class);

		conf.setMapperClass(DemoWordCountTuple.MapClass.class);
		conf.setCombinerClass(DemoWordCountTuple.ReduceClass.class);
		conf.setReducerClass(DemoWordCountTuple.ReduceClass.class);

		InMemoryOutputCollector collector = MapredHarness.run(conf);
		collector.printAll();
	}

}
