package edu.umd.cloud9.demo;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;

import edu.umd.cloud9.debug.InMemoryOutputCollector;
import edu.umd.cloud9.debug.MapredHarness;

public class LocalDemoWordCount {

	public static void main(String[] args) throws IOException {
		String filename = "../umd-hadoop-dist/sample-input/bible+shakes.nopunc";
		String outputPath = "sample-counts";
		int mapTasks = 20;
		int reduceTasks = 1;

		JobConf conf = new JobConf(DemoWordCount.class);
		conf.setJobName("wordcount");

		conf.setNumMapTasks(mapTasks);
		conf.setNumReduceTasks(reduceTasks);

		conf.setInputPath(new Path(filename));
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);
		conf.setOutputPath(new Path(outputPath));

		conf.setMapperClass(DemoWordCount.MyMapper.class);
		conf.setCombinerClass(DemoWordCount.MyReducer.class);
		conf.setReducerClass(DemoWordCount.MyReducer.class);

		InMemoryOutputCollector collector = MapredHarness.run(conf);
		collector.printAll();
	}

}
