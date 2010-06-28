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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.lib.IdentityReducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import edu.umd.cloud9.io.ArrayListOfIntsWritable;

/**
 * <p>
 * Driver program that takes a plain-text encoding of a directed graph and
 * builds corresponding Hadoop structures for representing the graph.
 * Command-line parameters are as follows:
 * </p>
 * 
 * <ul>
 * 
 * <li>[inputDir]: input directory</li>
 * <li>[outputDir]: output directory</li>
 * <li>[numNodes]: number of nodes in the graph</li>
 * 
 * </ul>
 * 
 * @author Jimmy Lin
 * @author Michael Schatz
 * 
 */
public class BuildPageRankRecords extends Configured implements Tool {

	private static final Logger sLogger = Logger.getLogger(BuildPageRankRecords.class);

	private static class MyMapper extends MapReduceBase implements
			Mapper<LongWritable, Text, IntWritable, PageRankNode> {

		private static IntWritable nid = new IntWritable();
		private static PageRankNode node = new PageRankNode();

		public void configure(JobConf job) {
			int n = job.getInt("NodeCnt", 0);
			node.setType(PageRankNode.TYPE_COMPLETE);
			node.setPageRank((float) -StrictMath.log(n));
		}

		public void map(LongWritable key, Text t,
				OutputCollector<IntWritable, PageRankNode> output, Reporter reporter)
				throws IOException {

			String[] arr = t.toString().trim().split("\\s+");

			nid.set(Integer.parseInt(arr[0]));
			if (arr.length == 1) {
				node.setNodeId(Integer.parseInt(arr[0]));
				node.setAdjacencyList(new ArrayListOfIntsWritable());

			} else {
				node.setNodeId(Integer.parseInt(arr[0]));

				int[] neighbors = new int[arr.length - 1];
				for (int i = 1; i < arr.length; i++) {
					neighbors[i - 1] = Integer.parseInt(arr[i]);
				}

				node.setAdjacencyList(new ArrayListOfIntsWritable(neighbors));
			}

			reporter.incrCounter("graph", "numNodes", 1);
			reporter.incrCounter("graph", "numEdges", arr.length - 1);

			if (arr.length > 1) {
				reporter.incrCounter("graph", "numActiveNodes", 1);
			}

			output.collect(nid, node);
		}
	}

	public BuildPageRankRecords() {
	}

	private static int printUsage() {
		System.out.println("usage: [inputDir] [outputDir] [numNodes]");
		ToolRunner.printGenericCommandUsage(System.out);
		return -1;
	}

	/**
	 * Runs this tool.
	 */
	public int run(String[] args) throws Exception {
		if (args.length != 3) {
			printUsage();
			return -1;
		}

		String inputPath = args[0];
		String outputPath = args[1];
		int n = Integer.parseInt(args[2]);

		sLogger.info("Tool name: BuildPageRankRecords");
		sLogger.info(" - inputDir: " + inputPath);
		sLogger.info(" - outputDir: " + outputPath);
		sLogger.info(" - numNodes: " + n);

		JobConf conf = new JobConf(BuildPageRankRecords.class);
		conf.setJobName("PackageLinkGraph");

		conf.setNumMapTasks(1);
		conf.setNumReduceTasks(0);

		conf.setInt("NodeCnt", n);
		conf.setInt("mapred.min.split.size", 1024 * 1024 * 1024);

		TextInputFormat.addInputPath(conf, new Path(inputPath));
		SequenceFileOutputFormat.setOutputPath(conf, new Path(outputPath));

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(SequenceFileOutputFormat.class);

		conf.setMapOutputKeyClass(IntWritable.class);
		conf.setMapOutputValueClass(PageRankNode.class);

		conf.setOutputKeyClass(IntWritable.class);
		conf.setOutputValueClass(PageRankNode.class);

		conf.setMapperClass(MyMapper.class);
		conf.setReducerClass(IdentityReducer.class);

		// delete the output directory if it exists already
		FileSystem.get(conf).delete(new Path(outputPath), true);

		JobClient.runJob(conf);

		return 0;
	}

	/**
	 * Dispatches command-line arguments to the tool via the
	 * <code>ToolRunner</code>.
	 */
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new BuildPageRankRecords(), args);
		System.exit(res);
	}
}
