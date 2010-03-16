package edu.umd.cloud9.collection.wikipedia;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.lib.IdentityReducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import edu.umd.cloud9.collection.Indexable;
import edu.umd.cloud9.collection.wikipedia.WikipediaPage;
import edu.umd.cloud9.collection.wikipedia.WikipediaPageInputFormat;
import edu.umd.cloud9.io.HMapSFW;

/**
 * @author ferhanture
 *
 */
@SuppressWarnings("deprecation")
public class SampleCollection  extends Configured implements Tool{
	private static final Logger sLogger = Logger.getLogger(SampleCollection.class);

	public SampleCollection(){

	}

	private static class MyMapper extends MapReduceBase implements
	Mapper<LongWritable, WikipediaPage, LongWritable, WikipediaPage> {
		static int sampleFreq;
		public void configure(JobConf conf){
			sampleFreq = conf.getInt("SampleFrequency", -1);
		}
		public void map(LongWritable key, WikipediaPage val,
				OutputCollector<LongWritable, WikipediaPage> output, Reporter reporter) throws IOException {
			if(key.get()%sampleFreq==0){			
				output.collect(key, val);
			}
		}
	}

	private static int printUsage() {
		System.out.println("usage: [input] [output-dir] [number-of-mappers] [sample-freq] [input-format]");
		ToolRunner.printGenericCommandUsage(System.out);
		return -1;
	}

	@SuppressWarnings("unchecked")
	public int run(String[] args) throws Exception {
		boolean isLocal = args.length==6;
		if (args.length != 5 && !isLocal) {
			printUsage();
			return -1;
		}
		String inputPath = args[0];
		String outputPath = args[1];
		int N = Integer.parseInt(args[2]);
		int sampleFreq = Integer.parseInt(args[3]);
		
		JobConf job = new JobConf(SampleCollection.class);
		job.setJobName(getClass().getName());

		int numMappers = N;
		int numReducers = 1;

		if (!FileSystem.get(job).exists(new Path(inputPath))) {
			throw new RuntimeException("Error, input path does not exist!");
		}

		sLogger.setLevel(Level.INFO);
		if (FileSystem.get(job).exists(new Path(outputPath))) {
			sLogger.info("Output path already exists!");
			return 0;
		}

		FileSystem.get(job).delete(new Path(outputPath), true);
		FileInputFormat.setInputPaths(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		FileOutputFormat.setCompressOutput(job, false);

		job.set("mapred.child.java.opts", "-Xmx2048m");
		job.setInt("mapred.map.max.attempts", 100);
		job.setInt("mapred.reduce.max.attempts", 100);
		job.setInt("mapred.task.timeout", 600000000);
		job.setInt("SampleFrequency", sampleFreq);
		if(isLocal){
			sLogger.info("Running local...");
			job.set("mapred.job.tracker", "local");
			job.set("fs.default.name", "file:///");
		}

		sLogger.info("Running job "+job.getJobName());
		sLogger.info("Input directory: "+inputPath);
		sLogger.info("Output directory: "+outputPath);
		sLogger.info("Number of mappers: "+N);
		sLogger.info("Sample frequency: "+sampleFreq);

		job.setNumMapTasks(numMappers);
		job.setNumReduceTasks(numReducers);
		job.setInputFormat(WikipediaPageInputFormat.class);
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(WikipediaPage.class);
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(WikipediaPage.class);
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(IdentityReducer.class);
		job.setOutputFormat(SequenceFileOutputFormat.class);

		JobClient.runJob(job); 		

		return 0;
	}

	public static void main(String[] args) throws Exception{
		ToolRunner.run(new Configuration(), new SampleCollection(), args);
		return;
	}
}
