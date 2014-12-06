package edu.umd.cloud9.example.memcached.demo;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapFile;
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
import org.apache.hadoop.mapred.lib.IdentityReducer;
import org.apache.pig.data.Tuple;

public class GetLogProbFromHDFS {

	/*
	 * This is used to add up total time for access to HDFS in map cycle
	 */
	static enum MyCounters {
		TIME;
	};

	/*
	 * Mapper class : Tokenizes the string and just accesses log probabilities
	 * from file on HDFS
	 */
	private static class MyMapper extends MapReduceBase implements
			Mapper<LongWritable, Tuple, LongWritable, FloatWritable> {

		Long keyTemp = new Long(0);
		Object obj;
		String inputPath;
		FileSystem fs;
		MapFile.Reader reader;

		/*
		 * This function opens a MAP-fomat file on HDFS to access log
		 * probabilities of the word
		 */
		public void configure(JobConf conf) {
			try {
				inputPath = conf.get("mapfileInputPath");
				fs = FileSystem.get(conf);
				reader = new MapFile.Reader(fs, inputPath, conf);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		/*
		 * Tokenizes the string. For each word, access the log probability of it
		 * from file on HDFS
		 */
		public void map(LongWritable key, Tuple value,
				OutputCollector<LongWritable, FloatWritable> output, Reporter reporter)
				throws IOException {
			FloatWritable logProb = new FloatWritable();
			FloatWritable finalValue = new FloatWritable();
			String line = (String) value.get(0);
			StringTokenizer itr = new StringTokenizer(line);

			float sum = 0;
			Text tempKey = new Text();
			// tokenize word in the line
			while (itr.hasMoreTokens()) {
				String temp = itr.nextToken();
				tempKey.set(temp);

				// start timer
				long startTime = System.currentTimeMillis();
				// access MAP file on HDFS
				reader.seek(tempKey);
				reader.get(tempKey, logProb);
				// end timer
				long endTime = System.currentTimeMillis();
				long diff = (endTime - startTime);
				// incrementing the count to access each single record
				reporter.incrCounter(MyCounters.TIME, diff);
				if (logProb == null)
					throw new RuntimeException("Error getting from memcache");

				sum += (float) logProb.get();
			}

			finalValue.set(sum);
			output.collect(key, finalValue);
		}
	}

	/*
	 * Takes in three arguments
	 */
	public static void main(String[] args) throws IOException {

		if (args.length != 3) {
			System.out
					.println(" usage : [path of sequence file on hdfs] [path of Map File on hdfs] [num of MapTasks]");
			System.exit(1);
		}

		String inputPath = args[0];
		String mapPath = args[1];
		int mapTasks = Integer.parseInt(args[2]);
		int reduceTasks = 0;

		String extraPath = "/shared/extraInfo";

		JobConf conf = new JobConf(GetLogProbFromHDFS.class);
		conf.setJobName("GetLogProbFromHDFS");

		conf.setNumMapTasks(mapTasks);
		conf.setNumReduceTasks(reduceTasks);

		FileInputFormat.setInputPaths(conf, new Path(inputPath));
		conf.setInputFormat(SequenceFileInputFormat.class);

		conf.setMapOutputValueClass(FloatWritable.class);
		conf.setMapperClass(MyMapper.class);
		conf.setReducerClass(IdentityReducer.class);
		// setting path to MAP file containing log probability so that it can be
		// accessed through mapper
		conf.set("mapfileInputPath", mapPath);

		Path outputDir = new Path(extraPath);
		FileSystem.get(conf).delete(outputDir, true);
		FileOutputFormat.setOutputPath(conf, outputDir);

		JobClient.runJob(conf);
	}

}
