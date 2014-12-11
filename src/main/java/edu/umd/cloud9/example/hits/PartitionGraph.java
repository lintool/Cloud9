package edu.umd.cloud9.example.hits;

import java.io.IOException;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.Partitioner;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import edu.umd.cloud9.example.hits.RangePartitioner;

/**
 * <p>
 * Driver program for partitioning the graph. This version reads & writes HITSNode writables. Command-line arguments are as
 * follows:
 * </p>
 * 
 * <ul>
 * 
 * <li>[inputDir]: input directory</li>
 * <li>[outputDir]: output directory</li>
 * <li>[numPartitions]: number of partitions</li>
 * <li>[useRange?]: 1 to use range partitioning or 0 otherwise</li>
 * <li>[nodeCount]: number of nodes in the graph</li>
 * 
 * </ul>
 * 
 * @author Jimmy Lin
 * @author Mike McGrath
 * 
 */

public class PartitionGraph extends Configured implements Tool {
	private static final Logger sLogger = Logger
			.getLogger(PartitionGraph.class);

	private static class MapClass extends MapReduceBase implements
			Mapper<IntWritable, HITSNode, IntWritable, HITSNode> {
		public void map(IntWritable nid, HITSNode node,
				OutputCollector<IntWritable, HITSNode> output, Reporter reporter)
				throws IOException {
			output.collect(nid, node);
		}
	}

	private static class ReduceClass extends MapReduceBase implements
			Reducer<IntWritable, HITSNode, IntWritable, HITSNode> {
		public void reduce(IntWritable nid, Iterator<HITSNode> values,
				OutputCollector<IntWritable, HITSNode> output, Reporter reporter)
				throws IOException {
			while (values.hasNext()) {
				HITSNode node = values.next();
				output.collect(nid, node);
			}
		}
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new PartitionGraph(),
				args);
		System.exit(res);
	}

	public PartitionGraph() {
	}

	private static int printUsage() {
		System.out
				.println("usage: [in-path] [out-path] [numPartitions] [useRange?] [nodeCount]");
		ToolRunner.printGenericCommandUsage(System.out);
		return -1;
	}

	public int run(String[] args) throws IOException {
		if (args.length != 5) {
			printUsage();
			return -1;
		}

		String inPath = args[0];
		String outPath = args[1];
		int numParts = Integer.parseInt(args[2]);
		boolean useRange = Integer.parseInt(args[3]) != 0;
		int nodeCount = Integer.parseInt(args[4]);

		sLogger.info("Tool name: PartitionGraph");
		sLogger.info(" - in dir: " + inPath);
		sLogger.info(" - out dir: " + outPath);
		sLogger.info(" - numParts: " + numParts);
		sLogger.info(" - useRange: " + useRange);
		sLogger.info(" - nodeCnt: " + nodeCount);

		JobConf conf = new JobConf(PartitionGraph.class);

		conf.setJobName("Partition Graph " + numParts);
		conf.setNumReduceTasks(numParts);

		conf.setInt("mapred.min.split.size", 1024 * 1024 * 1024);
		conf.set("mapred.child.java.opts", "-Xmx2048m");
		conf.setInt("NodeCount", nodeCount);

		FileInputFormat.setInputPaths(conf, new Path(inPath));
		FileOutputFormat.setOutputPath(conf, new Path(outPath));

		conf.setInputFormat(SequenceFileInputFormat.class);
		conf.setOutputFormat(SequenceFileOutputFormat.class);

		conf.setMapOutputKeyClass(IntWritable.class);
		conf.setMapOutputValueClass(HITSNode.class);

		conf.setOutputKeyClass(IntWritable.class);
		conf.setOutputValueClass(HITSNode.class);

		conf.setMapperClass(MapClass.class);
		conf.setReducerClass(ReduceClass.class);

		conf.setSpeculativeExecution(false);

		if (useRange) {
			conf.setPartitionerClass(RangePartitioner.class);
		}

		FileSystem.get(conf).delete(new Path(outPath), true);

		JobClient.runJob(conf);

		return 0;
	}
}
