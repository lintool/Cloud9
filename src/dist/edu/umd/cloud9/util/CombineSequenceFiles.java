package edu.umd.cloud9.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.lib.IdentityMapper;
import org.apache.hadoop.mapred.lib.IdentityReducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

/**
 * Combine a number of sequence files into a smaller number of sequence files. <p>
 * <p>
 * Input: Any number of sequence files containing key-value pairs, each of a certain type.<p>
 * Output: N sequence or text files containing all key-value pairs given as input.<p>
 * <p>
 * Given the number of desired sequence files, say N, map over a number of sequence files and partition all key-value pairs into N.<p>
 * <p>
 * Usage: [input] [output-dir] [number-of-mappers] [number-of-reducers] [key-class-name] [value-class-name] [seqeuence|text]<p>
 *<p> 
 * @author ferhanture
 *
 */
@SuppressWarnings("deprecation")
public class CombineSequenceFiles  extends Configured implements Tool{
	private static final Logger sLogger = Logger.getLogger(CombineSequenceFiles.class);

	public CombineSequenceFiles(){
		
	}
	
	private static int printUsage() {
		System.out.println("usage: [input] [output-dir] [number-of-mappers] [number-of-reducers] [key-class-name] [value-class-name] [seqeuence|text]");
//		ToolRunner.printGenericCommandUsage(System.out);
		return -1;
	}
	
	public int run(String[] args) throws Exception {
		if (args.length != 7 && args.length!=8) {
			printUsage();
			return -1;
		}
		String inputPath = args[0];
		String outputPath = args[1];
		int numMappers = Integer.parseInt(args[2]);
		int numReducers = Integer.parseInt(args[3]);
		String keyClassName = args[4];
		String valueClassName = args[5];

		Class keyClass, valueClass;
		try {
			keyClass = Class.forName(keyClassName);
			valueClass = Class.forName(valueClassName);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
			throw new RuntimeException("Class not found: "+keyClassName + "," + valueClassName);
		}
		
		JobConf job = new JobConf(CombineSequenceFiles.class);
		job.setJobName("CombineSequenceFiles");
			
		FileSystem.get(job).delete(new Path(outputPath), true);
		
		FileStatus[] stat = FileSystem.get(job).listStatus(new Path(inputPath));
		for (int i = 0; i < stat.length; ++i) {
			FileInputFormat.addInputPath(job, stat[i].getPath());
			sLogger.info("Added: "+stat[i].getPath());
		}
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		FileOutputFormat.setCompressOutput(job, false);

		job.set("mapred.child.java.opts", "-Xmx2048m");
		job.setInt("mapred.map.max.attempts", 100);
		job.setInt("mapred.reduce.max.attempts", 100);
		job.setInt("mapred.task.timeout", 600000000);
		if(args.length==8){
			job.set("mapred.job.tracker", "local");
			job.set("fs.default.name", "file:///");
		}
		sLogger.setLevel(Level.INFO);
		
		sLogger.info("Running job "+job.getJobName());
		sLogger.info("Input directory: "+inputPath);
		sLogger.info("Output directory: "+outputPath);
		sLogger.info("Number of mappers: "+numMappers);
		sLogger.info("Number of reducers: "+numReducers);
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
		if (args[6].equals("sequence")) {
		  job.setOutputFormat(SequenceFileOutputFormat.class);
		}else if (args[6].equals("text")) {
      job.setOutputFormat(TextOutputFormat.class);		  
		}else {
      throw new RuntimeException("Unknown output format: "+args[6]);
		}

		JobClient.runJob(job); 		

		return 0;
	}

	public static void main(String[] args) throws Exception{
		ToolRunner.run(new Configuration(), new CombineSequenceFiles(), args);
		return;
	}
}
