package edu.umd.cloud9.example.memcached.demo;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.lib.IdentityReducer;

public class ComputeLogProb {

	public static class MyMapper extends MapReduceBase implements
			Mapper<Text, IntWritable, Text, FloatWritable> {

		// reuse objects to save overhead of object creation
		private final static FloatWritable theFloat = new FloatWritable();
		private long totalWords;

		public void configure(JobConf job) {
			totalWords = job.getLong("cnt", -1);
		}

		public void map(Text term, IntWritable cnt, OutputCollector<Text, FloatWritable> output,
				Reporter reporter) throws IOException {

			double d = (double) cnt.get() / (double) totalWords;
			double logp = Math.log(d);
			theFloat.set((float) logp);

			output.collect(term, theFloat);
		}
	}

	protected ComputeLogProb() {
	}

	/**
	 * Runs the demo.
	 */
	public static void main(String[] args) throws IOException {
		if (args.length != 4) {
			System.out.println("usage: [input] [output] [num-mappers] [cnt]");
			System.exit(-1);
		}

		String inputPath = args[0];
		String outputPath = args[1];

		int mapTasks = Integer.parseInt(args[2]);
		int reduceTasks = 1;
		
		long cnt = Long.parseLong(args[3]);

		JobConf conf = new JobConf(ComputeLogProb.class);
		conf.setJobName("WordCount");

		conf.setNumMapTasks(mapTasks);
		conf.setNumReduceTasks(reduceTasks);
		
		conf.setLong("cnt", cnt);

		FileInputFormat.setInputPaths(conf, new Path(inputPath));
		FileOutputFormat.setOutputPath(conf, new Path(outputPath));
		FileOutputFormat.setCompressOutput(conf, false);

		conf.setInputFormat(SequenceFileInputFormat.class);
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(FloatWritable.class);
		conf.setOutputFormat(SequenceFileOutputFormat.class);

		conf.setMapperClass(MyMapper.class);
		conf.setCombinerClass(IdentityReducer.class);
		conf.setReducerClass(IdentityReducer.class);

		// Delete the output directory if it exists already
		Path outputDir = new Path(outputPath);
		FileSystem.get(conf).delete(outputDir, true);

		JobClient.runJob(conf);
	}
}
