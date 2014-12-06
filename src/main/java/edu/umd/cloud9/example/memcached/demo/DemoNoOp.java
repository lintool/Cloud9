package edu.umd.cloud9.example.memcached.demo;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.lib.IdentityReducer;

public class DemoNoOp {

	private static class MyMapper extends MapReduceBase implements
			Mapper<LongWritable, Text, LongWritable, FloatWritable> {

		public void map(LongWritable key, Text value,
				OutputCollector<LongWritable, FloatWritable> output, Reporter reporter)
				throws IOException {
			FloatWritable tempFloat = new FloatWritable();
			FloatWritable finalValue = new FloatWritable();

			// get the actual sentence from the Tuple field
			String line = value.toString();

			StringTokenizer itr = new StringTokenizer(line);
			float sum = 0;
			Text tempKey = new Text();

			// loop through all the words in the sentence
			while (itr.hasMoreTokens()) {
				String temp = itr.nextToken();
				tempKey.set(temp);
				tempFloat.set(0);
				sum += (float) tempFloat.get();
			}

			finalValue.set(sum);
			output.collect(key, finalValue);
		}
	}

	public static void main(String[] args) throws IOException {

		if (args.length != 2) {
			System.out.println(" usage : [path of sequence file on hdfs] [num of MapTasks]");
			System.exit(1);
		}

		String inputPath = args[0];
		int mapTasks = Integer.parseInt(args[1]);
		int reduceTasks = 0;

		String extraPath = "/tmp";

		JobConf conf = new JobConf(DemoNoOp.class);
		conf.setJobName("DemoNoOp");

		conf.setNumMapTasks(mapTasks);
		conf.setNumReduceTasks(reduceTasks);

		FileInputFormat.setInputPaths(conf, new Path(inputPath));
		conf.setInputFormat(TextInputFormat.class);

		conf.setMapOutputValueClass(FloatWritable.class);
		conf.setMapperClass(MyMapper.class);
		conf.setReducerClass(IdentityReducer.class);

		Path outputDir = new Path(extraPath);
		FileSystem.get(conf).delete(outputDir, true);
		FileOutputFormat.setOutputPath(conf, outputDir);

		long startTime = System.currentTimeMillis();
		JobClient.runJob(conf);
		long endTime = System.currentTimeMillis();
		long diff = (endTime - startTime);

		System.out.println("Total job completion time (ms): " + diff);
	}
}
