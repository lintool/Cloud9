/*
 * Cloud9: A MapReduce Library for Hadoop
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You may
 * obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package edu.umd.cloud9.pagerank;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
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
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

/**
 * <p>
 * Driver program for partitioning the graph. Command-line arguments are as
 * follows:
 * </p>
 * 
 * <ul>
 * 
 * <li>[inputDir]: input directory</li>
 * <li>[outputDir]: output directory</li>
 * <li>[numPartitions]: number of partitions</li>
 * <li>[useRange?]: 1 to user range partitioning or 0 otherwise</li>
 * <li>[nodeCount]: number of nodes in the graph</li>
 * 
 * </ul>
 * 
 * @author Jimmy Lin
 * @author Michael Schatz
 * 
 */
public class PartitionGraph extends Configured implements Tool {
	private static final Logger sLogger = Logger.getLogger(PartitionGraph.class);

	private static class MapClass extends MapReduceBase implements
			Mapper<IntWritable, PageRankNode, IntWritable, PageRankNode> {
		public void map(IntWritable nid, PageRankNode node,
				OutputCollector<IntWritable, PageRankNode> output, Reporter reporter)
				throws IOException {
			output.collect(nid, node);
		}
	}

	private static class ReduceClass extends MapReduceBase implements
			Reducer<IntWritable, PageRankNode, IntWritable, PageRankNode> {
		public void reduce(IntWritable nid, Iterator<PageRankNode> values,
				OutputCollector<IntWritable, PageRankNode> output, Reporter reporter)
				throws IOException {
			while (values.hasNext()) {
				PageRankNode node = values.next();
				output.collect(nid, node);
			}
		}
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new PartitionGraph(), args);
		System.exit(res);
	}

	public PartitionGraph() {
	}

	private static int printUsage() {
		System.out.println("usage: [inputDir] [outputDir] [numPartitions] [useRange?] [nodeCount]");
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
		sLogger.info(" - inputDir: " + inPath);
		sLogger.info(" - outputDir: " + outPath);
		sLogger.info(" - numPartitions: " + numParts);
		sLogger.info(" - useRange?: " + useRange);
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
		conf.setMapOutputValueClass(PageRankNode.class);

		conf.setOutputKeyClass(IntWritable.class);
		conf.setOutputValueClass(PageRankNode.class);

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
