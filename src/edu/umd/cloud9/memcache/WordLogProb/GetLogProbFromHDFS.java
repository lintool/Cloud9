package edu.umd.cloud9.memcache.WordLogProb;

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

import edu.umd.cloud9.io.Tuple;

public class GetLogProbFromHDFS {
	static enum MyCounters {
		TIME;
	};

	public static class MyMapper extends MapReduceBase implements
			Mapper<LongWritable, Tuple, LongWritable, FloatWritable> {

		Long keyTemp = new Long(0);
		Object obj;
		String inputPath;
		FileSystem fs;
		MapFile.Reader reader;

		public void configure(JobConf conf) {
			try {
				inputPath = conf.get("mapfileInputPath");
				fs = FileSystem.get(conf);
				reader = new MapFile.Reader(fs,inputPath, conf);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		public void map(LongWritable key, Tuple value,
				OutputCollector<LongWritable, FloatWritable> output, Reporter reporter)
				throws IOException {
			FloatWritable fw = new FloatWritable();
			FloatWritable finalValue = new FloatWritable();
			String line = (String) value.get(0);
			StringTokenizer itr = new StringTokenizer(line);

			float sum = 0;
			Text tempKey = new Text();

			while (itr.hasMoreTokens()) {
				String temp = itr.nextToken();
				tempKey.set(temp);

				long startTime = System.currentTimeMillis();
				reader.seek(tempKey);
				reader.get(tempKey, fw);
				long endTime = System.currentTimeMillis();
				long diff = (endTime - startTime);
				reporter.incrCounter(MyCounters.TIME, diff);
				if (fw == null)
					throw new RuntimeException("Error getting from memcache");

				sum += (float) fw.get();
			}

			finalValue.set(sum);
			output.collect(key, finalValue);
		}
	}

	public static void main(String[] args) throws IOException {

		if (args.length != 3) {
			System.out.println(" usage : [path of sequence file on hdfs] [path of Map File] [num of MapTasks]");
			System.exit(1);
		}
		
		String inputPath = args[0];
		String mapPath = args[1];
		int mapTasks = Integer.parseInt(args[2]);
		int reduceTasks = 0;
		
		String extraPath = "/shared/extraInfo";

		JobConf conf = new JobConf(GetLogProbFromHDFS.class);
		conf.setJobName("GetFromFS");

		conf.setNumMapTasks(mapTasks);
		conf.setNumReduceTasks(reduceTasks);

		FileInputFormat.setInputPaths(conf, new Path(inputPath));
		conf.setInputFormat(SequenceFileInputFormat.class);

		conf.setMapOutputValueClass(FloatWritable.class);
		conf.setMapperClass(MyMapper.class);
		conf.setReducerClass(IdentityReducer.class);
		conf.set("mapfileInputPath", mapPath);

		Path outputDir = new Path(extraPath);
		FileSystem.get(conf).delete(outputDir, true);
		FileOutputFormat.setOutputPath(conf, outputDir);

		long startTime = System.currentTimeMillis();
		JobClient.runJob(conf);
		long endTime = System.currentTimeMillis();
		float diff = (float) ((endTime - startTime));
		System.out.println("\n Starttime " + startTime + " end time = " + endTime);
		System.out.println("\n Total time taken is : " + diff);
	}

}
