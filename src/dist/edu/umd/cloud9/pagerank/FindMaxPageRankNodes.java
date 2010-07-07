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
import java.util.PriorityQueue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
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
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

public class FindMaxPageRankNodes extends Configured implements Tool {

	private static final Logger sLogger = Logger.getLogger(FindMaxPageRankNodes.class);

	private static class NodeRanking implements Comparable<NodeRanking> {

		private int nid;
		private float score;

		public NodeRanking(int n, float s) {
			nid = n;
			score = s;
		}

		public int getNodeId() {
			return nid;
		}

		public float getPageRank() {
			return score;
		}

		public int compareTo(NodeRanking that) {
			if (this.getPageRank() < that.getPageRank()) {
				return -1;
			}

			if (this.getPageRank() > that.getPageRank())
				return 1;

			if (this.getNodeId() < that.getNodeId()) {
				return -1;
			}

			if (this.getNodeId() > that.getNodeId()) {
				return 1;
			}

			return 0;
		}
	}

	private static class MyMapper extends MapReduceBase implements
			Mapper<IntWritable, PageRankNode, IntWritable, FloatWritable> {

		private static OutputCollector<IntWritable, FloatWritable> output;
		private static PriorityQueue<NodeRanking> q = new PriorityQueue<NodeRanking>();

		private int n;
		
		public void configure(JobConf job) {
			n = job.getInt("n", 100);
		}

		public void map(IntWritable nid, PageRankNode node,
				OutputCollector<IntWritable, FloatWritable> output, Reporter reporter)
				throws IOException {

			this.output = output;

			if (q.size() < n) {
				q.add(new NodeRanking(node.getNodeId(), node.getPageRank()));
			} else {
				if (q.peek().getPageRank() < node.getPageRank()) {
					q.poll();
					q.add(new NodeRanking(node.getNodeId(), node.getPageRank()));
				}
			}
		}

		public void close() throws IOException {
			IntWritable k = new IntWritable();
			FloatWritable v = new FloatWritable();

			NodeRanking n;
			while ((n = q.poll()) != null) {
				sLogger.info(n.getPageRank() + "\t" + n.getNodeId());

				k.set(n.getNodeId());
				v.set(n.getPageRank());
				output.collect(k, v);
			}
		}
	}

	private static class MyReducer extends MapReduceBase implements
			Reducer<IntWritable, FloatWritable, IntWritable, FloatWritable> {

		private static OutputCollector<IntWritable, FloatWritable> output;
		private static PriorityQueue<NodeRanking> q = new PriorityQueue<NodeRanking>();

		private int n = 100;
		
		public void configure(JobConf job) {
			n = job.getInt("n", 100);
		}

		public void reduce(IntWritable nid, Iterator<FloatWritable> iter,
				OutputCollector<IntWritable, FloatWritable> output, Reporter reporter)
				throws IOException {

			this.output = output;

			FloatWritable p = iter.next();
			if (q.size() < n) {
				q.add(new NodeRanking(nid.get(), p.get()));
			} else {
				if (q.peek().getPageRank() < p.get()) {
					q.poll();
					q.add(new NodeRanking(nid.get(), p.get()));
				}
			}
		}

		public void close() throws IOException {
			IntWritable k = new IntWritable();
			FloatWritable v = new FloatWritable();

			NodeRanking n;
			while ((n = q.poll()) != null) {
				sLogger.info(n.getPageRank() + "\t" + n.getNodeId());

				k.set(n.getNodeId());
				v.set(n.getPageRank());
				output.collect(k, v);
			}
		}
	}

	public FindMaxPageRankNodes() {
	}

	private static int printUsage() {
		System.out.println("usage: [input] [output] [n]");
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
		
		sLogger.info("Tool name: FindMaxPageRankNodes");
		sLogger.info(" - input: " + inputPath);
		sLogger.info(" - output: " + outputPath);
		sLogger.info(" - n: " + n);

		JobConf conf = new JobConf(FindMaxPageRankNodes.class);
		conf.setJobName("FindMaxPageRankNodes");

		conf.setNumMapTasks(1);
		conf.setNumReduceTasks(1);

		conf.setInt("mapred.min.split.size", 1024 * 1024 * 1024);
		conf.setInt("n", n);

		FileInputFormat.addInputPath(conf, new Path(inputPath));
		FileOutputFormat.setOutputPath(conf, new Path(outputPath));

		conf.setInputFormat(SequenceFileInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		conf.setMapOutputKeyClass(IntWritable.class);
		conf.setMapOutputValueClass(FloatWritable.class);

		conf.setOutputKeyClass(IntWritable.class);
		conf.setOutputValueClass(FloatWritable.class);

		conf.setMapperClass(MyMapper.class);
		conf.setReducerClass(MyReducer.class);

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
		int res = ToolRunner.run(new Configuration(), new FindMaxPageRankNodes(), args);
		System.exit(res);
	}
}
