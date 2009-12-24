package edu.umd.cloud9.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.lib.IdentityMapper;
import org.apache.hadoop.mapred.lib.IdentityReducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

@SuppressWarnings("deprecation")
public class CombineSequenceFiles  extends Configured implements Tool{
	private static final Logger sLogger = Logger.getLogger(CombineSequenceFiles.class);

	public CombineSequenceFiles(){
		
	}
	
	private static int printUsage() {
		System.out.println("usage: [input] [output-dir] [number-of-mappers] [key-class-name] [value-class-name]");
		ToolRunner.printGenericCommandUsage(System.out);
		return -1;
	}
	
	public int run(String[] args) throws Exception {
		if (args.length != 5 && args.length!=6) {
			printUsage();
			return -1;
		}
		String inputPath = args[0];
		String outputPath = args[1];
		int N = Integer.parseInt(args[2]);
		String keyClassName = args[3];
		String valueClassName = args[4];

		Class keyClass, valueClass;
		try {
			keyClass = Class.forName(keyClassName);
			valueClass = Class.forName(valueClassName);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
			throw new RuntimeException("Class not found");
		}
		
		JobConf job = new JobConf(CombineSequenceFiles.class);
		job.setJobName("CombineSequenceFiles");
			
		int numMappers = N;
		int numReducers = 1;

		FileSystem.get(job).delete(new Path(outputPath), true);
		FileInputFormat.setInputPaths(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		FileOutputFormat.setCompressOutput(job, false);

		job.set("mapred.child.java.opts", "-Xmx2048m");
		job.setInt("mapred.map.max.attempts", 100);
		job.setInt("mapred.reduce.max.attempts", 100);
		job.setInt("mapred.task.timeout", 600000000);
		if(args.length==6){
			job.set("mapred.job.tracker", "local");
			job.set("fs.default.name", "file:///");
		}
		sLogger.setLevel(Level.INFO);
		
		sLogger.info("Running job "+job.getJobName());
		sLogger.info("Input directory: "+inputPath);
		sLogger.info("Output directory: "+outputPath);
		sLogger.info("Number of mappers: "+N);
		sLogger.info("Key class: "+keyClass.getName());
		sLogger.info("Value class: "+valueClass.getName());

		
		job.setNumMapTasks(numMappers);
		job.setNumReduceTasks(numReducers);
		job.setInputFormat(SequenceFileInputFormat.class);
		job.setMapOutputKeyClass(keyClass);
		job.setMapOutputValueClass(valueClass);
		job.setOutputKeyClass(keyClass);
		job.setOutputValueClass(valueClass);
		job.setMapperClass(IdentityMapper.class);
		job.setReducerClass(IdentityReducer.class);
		job.setOutputFormat(SequenceFileOutputFormat.class);

		JobClient.runJob(job); 		

		return 0;
	}

	public static void main(String[] args) throws Exception{
		ToolRunner.run(new Configuration(), new CombineSequenceFiles(), args);
		return;
	}
}
