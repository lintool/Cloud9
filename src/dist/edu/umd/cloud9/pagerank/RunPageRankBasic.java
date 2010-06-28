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
import org.apache.hadoop.mapred.lib.IdentityReducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import edu.umd.cloud9.io.ArrayListOfIntsWritable;
import edu.umd.cloud9.util.HMapIF;
import edu.umd.cloud9.util.MapIF;

/**
 * <p>
 * Main driver program for running the basic (non-Schimmy) implementation of
 * PageRank. Command-line arguments are as follows:
 * </p>
 * 
 * <ul>
 * <li>[basePath]: the base path</li>
 * <li>[numNodes]: number of nodes in the graph</li>
 * <li>[start]: starting iteration</li>
 * <li>[end]: ending iteration</li>
 * <li>[useCombiner?]: 1 for using combiner, 0 for not</li>
 * <li>[useInMapCombiner?]: 1 for using in-mapper combining, 0 for not</li>
 * <li>[useRange?]: 1 for range partitioning, 0 for not</li>
 * </ul>
 * 
 * <p>
 * The starting and ending iterations will correspond to paths
 * <code>/base/path/iterXXXX</code> and <code>/base/path/iterYYYY</code>. As a
 * example, if you specify 0 and 10 as the starting and ending iterations, the
 * driver program will start with the graph structure stored at
 * <code>/base/path/iter0000</code>; final results will be stored at
 * <code>/base/path/iter0010</code>.
 * </p>
 * 
 * @see RunPageRankSchimmy
 * @author Jimmy Lin
 * @author Michael Schatz
 * 
 */
public class RunPageRankBasic extends Configured implements Tool {

	private static final Logger sLogger = Logger.getLogger(RunPageRankBasic.class);

	// mapper, no in-mapper combining
	private static class MapClass extends MapReduceBase implements
			Mapper<IntWritable, PageRankNode, IntWritable, PageRankNode> {

		// the neighbor to which we're sending messages
		private static IntWritable sNeighbor = new IntWritable();

		// contents of the messages: partial PageRank mass
		private static PageRankNode sIntermediateMass = new PageRankNode();

		// for passing along node structure
		private static PageRankNode sIntermediateStructure = new PageRankNode();

		public void map(IntWritable nid, PageRankNode node,
				OutputCollector<IntWritable, PageRankNode> output, Reporter reporter)
				throws IOException {

			// pass along node structure
			sIntermediateStructure.setNodeId(node.getNodeId());
			sIntermediateStructure.setType(PageRankNode.TYPE_STRUCTURE);
			sIntermediateStructure.setAdjacencyList(node.getAdjacenyList());

			output.collect(nid, sIntermediateStructure);

			int massMessages = 0;

			// distribute PageRank mass to neighbors (along outgoing edges)
			if (node.getAdjacenyList().size() > 0) {
				// each neighbor gets an equal share of PageRank mass
				ArrayListOfIntsWritable list = node.getAdjacenyList();
				float mass = node.getPageRank() - (float) StrictMath.log(list.size());

				// iterate over neighbors
				for (int i = 0; i < list.size(); i++) {
					sNeighbor.set(list.get(i));
					sIntermediateMass.setNodeId(list.get(i));
					sIntermediateMass.setType(PageRankNode.TYPE_MASS);
					sIntermediateMass.setPageRank(mass);

					// emit messages with PageRank mass to neighbors
					output.collect(sNeighbor, sIntermediateMass);
					massMessages++;
				}
			}

			// bookkeeping
			reporter.incrCounter("PageRank", "nodes", 1);
			reporter.incrCounter("PageRank", "massMessages", massMessages);
		}
	}

	// mapper with in-mapper combiner optimization
	private static class MapWithInMapperCombiningClass extends MapReduceBase implements
			Mapper<IntWritable, PageRankNode, IntWritable, PageRankNode> {

		// save a reference to the output collector
		private static OutputCollector<IntWritable, PageRankNode> mOutput;

		// for buffering PageRank mass contributes keyed by destination node
		private static HMapIF map = new HMapIF();

		// for passing along node structure
		private static PageRankNode sIntermediateStructure = new PageRankNode();

		public void map(IntWritable nid, PageRankNode node,
				OutputCollector<IntWritable, PageRankNode> output, Reporter reporter)
				throws IOException {
			mOutput = output;

			// pass along node structure
			sIntermediateStructure.setNodeId(node.getNodeId());
			sIntermediateStructure.setType(PageRankNode.TYPE_STRUCTURE);
			sIntermediateStructure.setAdjacencyList(node.getAdjacenyList());

			output.collect(nid, sIntermediateStructure);

			int massMessages = 0;
			int massMessagesSaved = 0;

			// distribute PageRank mass to neighbors (along outgoing edges)
			if (node.getAdjacenyList().size() > 0) {
				// each neighbor gets an equal share of PageRank mass
				ArrayListOfIntsWritable list = node.getAdjacenyList();
				float mass = node.getPageRank() - (float) StrictMath.log(list.size());

				// iterate over neighbors
				for (int i = 0; i < list.size(); i++) {
					int neighbor = list.get(i);

					if (map.containsKey(neighbor)) {
						// already message destined for that node; add PageRank
						// mass contribution
						massMessagesSaved++;
						map.put(neighbor, sumLogProbs(map.get(neighbor), mass));
					} else {
						// new destination node
						massMessages++;
						map.put(neighbor, mass);
					}
				}
			}

			// bookkeeping
			reporter.incrCounter("PageRank", "nodes", 1);
			reporter.incrCounter("PageRank", "massMessages", massMessages);
			reporter.incrCounter("PageRank", "massMessagesSaved", massMessagesSaved);
		}

		public void close() throws IOException {
			// now emit the messages all at once
			IntWritable k = new IntWritable();
			PageRankNode mass = new PageRankNode();

			for (MapIF.Entry e : map.entrySet()) {
				k.set(e.getKey());

				mass.setNodeId(e.getKey());
				mass.setType(PageRankNode.TYPE_MASS);
				mass.setPageRank(e.getValue());

				mOutput.collect(k, mass);
			}
		}
	}

	// combiner: sums partial PageRank contributions and passes node structure
	// along
	private static class CombineClass extends MapReduceBase implements
			Reducer<IntWritable, PageRankNode, IntWritable, PageRankNode> {

		private static PageRankNode sIntermediateMass = new PageRankNode();

		public void reduce(IntWritable nid, Iterator<PageRankNode> values,
				OutputCollector<IntWritable, PageRankNode> output, Reporter reporter)
				throws IOException {

			int massMessages = 0;

			// remember, PageRank mass is stored as a log prob
			float mass = Float.NEGATIVE_INFINITY;
			while (values.hasNext()) {
				PageRankNode n = values.next();

				if (n.getType() == PageRankNode.TYPE_STRUCTURE) {
					// simply pass along node structure
					output.collect(nid, n);
				} else {
					// accumulate PageRank mass contributions
					mass = sumLogProbs(mass, n.getPageRank());
					massMessages++;
				}
			}

			// emit aggregated results
			if (massMessages > 0) {
				sIntermediateMass.setNodeId(nid.get());
				sIntermediateMass.setType(PageRankNode.TYPE_MASS);
				sIntermediateMass.setPageRank(mass);

				output.collect(nid, sIntermediateMass);
			}
		}
	}

	// reduce: sums incoming PageRank contributions, rewrite graph structure
	private static class ReduceClass extends MapReduceBase implements
			Reducer<IntWritable, PageRankNode, IntWritable, PageRankNode> {

		private JobConf mJobConf;
		private String mTaskId;
		private String mPath;

		// for keeping track of PageRank mass encountered, so we can compute
		// missing PageRank mass lost through dangling nodes
		private float mTotalMass = Float.NEGATIVE_INFINITY;

		public void configure(JobConf job) {
			// state required for writing out amount of PageRank mass
			// encountered in the reducer later
			mJobConf = job;
			mTaskId = job.get("mapred.task.id");
			mPath = job.get("PageRankMassPath");
		}

		public void reduce(IntWritable nid, Iterator<PageRankNode> values,
				OutputCollector<IntWritable, PageRankNode> output, Reporter reporter)
				throws IOException {

			// create the node structure that we're going to assemble back
			// together from shuffled pieces
			PageRankNode node = new PageRankNode();

			node.setType(PageRankNode.TYPE_COMPLETE);
			node.setNodeId(nid.get());

			int massMessagesReceived = 0;
			int structureReceived = 0;

			float mass = Float.NEGATIVE_INFINITY;
			while (values.hasNext()) {
				PageRankNode n = values.next();

				if (n.getType() == PageRankNode.TYPE_STRUCTURE) {
					// this is the structure; update accordingly
					ArrayListOfIntsWritable list = n.getAdjacenyList();
					structureReceived++;

					int arr[] = new int[list.size()];
					for (int i = 0; i < list.size(); i++) {
						arr[i] = list.get(i);
					}

					node.setAdjacencyList(new ArrayListOfIntsWritable(arr));
				} else {
					// this is a message that contains PageRank mass; accumulate
					mass = sumLogProbs(mass, n.getPageRank());
					massMessagesReceived++;

					mTotalMass = sumLogProbs(mTotalMass, n.getPageRank());
				}
			}

			// update the final accumulated PageRank mass
			node.setPageRank(mass);
			reporter.incrCounter("PageRank", "massMessagesReceived", massMessagesReceived);

			// error checking
			if (structureReceived == 1) {
				// everything checks out, emit final node structure with updated
				// PageRank value
				output.collect(nid, node);
			} else if (structureReceived == 0) {
				reporter.incrCounter("PageRank", "noStructure", 1);
				System.err.print("No structure received for nodeid: " + nid.get() + " mass: "
						+ massMessagesReceived);
			} else {
				reporter.incrCounter("PageRank", "multipleStructure", 1);
				System.err.print("Multiple structure received for nodeid: " + nid.get() + " mass: "
						+ massMessagesReceived + " struct: " + structureReceived);
			}
		}

		public void close() throws IOException {
			// write to a file the amount of PageRank mass we've seen in this
			// reducer
			FileSystem fs = FileSystem.get(mJobConf);
			Path path = new Path(mPath + "/" + mTaskId);
			FSDataOutputStream out = fs.create(path, false);
			out.writeFloat(mTotalMass);
			out.close();
		}
	}

	// mapper that distributes the missing PageRank mass (lost at the dangling
	// nodes) and takes care of the random jump factor.
	private static class MapPageRankMassDistributionClass extends MapReduceBase implements
			Mapper<IntWritable, PageRankNode, IntWritable, PageRankNode> {

		private float mMissingMass = 0.0f;
		private int mNodeCnt = 0;

		public void configure(JobConf job) {
			mMissingMass = job.getFloat("MissingMass", 0.0f);
			mNodeCnt = job.getInt("NodeCount", 0);
		}

		public void map(IntWritable nid, PageRankNode node,
				OutputCollector<IntWritable, PageRankNode> output, Reporter reporter)
				throws IOException {

			float p = node.getPageRank();

			float jump = (float) (Math.log(mAlpha) - Math.log(mNodeCnt));
			float link = (float) Math.log(1.0f - mAlpha)
					+ sumLogProbs(p, (float) (Math.log(mMissingMass) - Math.log(mNodeCnt)));

			p = sumLogProbs(jump, link);
			node.setPageRank(p);

			output.collect(nid, node);
		}
	}

	// random jump factor
	private static float mAlpha = 0.15f;

	private NumberFormat sFormat = new DecimalFormat("0000");

	/**
	 * Dispatches command-line arguments to the tool via the
	 * <code>ToolRunner</code>.
	 */
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new RunPageRankBasic(), args);
		System.exit(res);
	}

	public RunPageRankBasic() {
	}

	private static int printUsage() {
		System.out
				.println("usage: [basePath] [numNodes] [start] [end] [useCombiner?] [useInMapCombiner?] [useRange?]");
		ToolRunner.printGenericCommandUsage(System.out);
		return -1;
	}

	/**
	 * Runs this tool.
	 */
	public int run(String[] args) throws Exception {

		if (args.length != 7) {
			printUsage();
			return -1;
		}

		String basePath = args[0];
		int n = Integer.parseInt(args[1]);
		int s = Integer.parseInt(args[2]);
		int e = Integer.parseInt(args[3]);
		boolean useCombiner = Integer.parseInt(args[4]) != 0;
		boolean useInmapCombiner = Integer.parseInt(args[5]) != 0;
		boolean useRange = Integer.parseInt(args[6]) != 0;

		sLogger.info("Tool name: RunPageRank");
		sLogger.info(" - basePath: " + basePath);
		sLogger.info(" - numNodes: " + n);
		sLogger.info(" - start iteration: " + s);
		sLogger.info(" - end iteration: " + e);
		sLogger.info(" - useCombiner?: " + useCombiner);
		sLogger.info(" - useInMapCombiner?: " + useInmapCombiner);
		sLogger.info(" - useRange?: " + useRange);

		// iterate PageRank
		for (int i = s; i < e; i++) {
			iteratePageRank(basePath, i, i + 1, n, useCombiner, useInmapCombiner, useRange);
		}

		return 0;
	}

	// run each iteration
	private void iteratePageRank(String path, int i, int j, int n, boolean useCombiner,
			boolean useInmapCombiner, boolean useRange) throws IOException {
		// each iteration consists of two phases (two MapReduce jobs)...

		// job1: distribute PageRank mass along outgoing edges
		float mass = phase1(path, i, j, n, useCombiner, useInmapCombiner, useRange);

		// find out how much PageRank mass got lost at the dangling nodes
		float missing = 1.0f - (float) StrictMath.exp(mass);

		// job2: distribute missing mass, take care of random jump factor
		phase2(path, i, j, n, missing);
	}

	private float phase1(String path, int i, int j, int n, boolean useCombiner,
			boolean useInmapCombiner, boolean useRange) throws IOException {
		JobConf conf = new JobConf(RunPageRankBasic.class);

		String in = path + "/iter" + sFormat.format(i);
		String out = path + "/iter" + sFormat.format(j) + "t";
		String outm = out + "-mass";

		// we need to actually count the number of part files to get the number
		// of partitions (because the directory might contain _log)
		int numPartitions = 0;
		for (FileStatus s : FileSystem.get(conf).listStatus(new Path(in))) {
			if (s.getPath().getName().contains("part-"))
				numPartitions++;
		}

		sLogger.info("PageRank: iteration " + j + ": Phase1");
		sLogger.info(" - input: " + in);
		sLogger.info(" - output: " + out);
		sLogger.info(" - nodeCnt: " + n);
		sLogger.info(" - useCombiner: " + useCombiner);
		sLogger.info(" - useInmapCombiner: " + useInmapCombiner);
		sLogger.info(" - useRange: " + useRange);
		sLogger.info("computed number of partitions: " + numPartitions);

		int numMapTasks = numPartitions;
		int numReduceTasks = numPartitions;

		conf.setJobName("PageRank:Basic:iteration" + j + ":Phase1");
		conf.setInt("NodeCount", n);

		conf.setNumMapTasks(numMapTasks);
		conf.setNumReduceTasks(numReduceTasks);

		conf.setInt("mapred.min.split.size", 1024 * 1024 * 1024);
		conf.set("mapred.child.java.opts", "-Xmx2048m");

		conf.set("PageRankMassPath", outm);

		FileInputFormat.setInputPaths(conf, new Path(in));
		FileOutputFormat.setOutputPath(conf, new Path(out));

		conf.setInputFormat(SequenceFileInputFormat.class);
		conf.setOutputFormat(SequenceFileOutputFormat.class);

		conf.setMapOutputKeyClass(IntWritable.class);
		conf.setMapOutputValueClass(PageRankNode.class);

		conf.setOutputKeyClass(IntWritable.class);
		conf.setOutputValueClass(PageRankNode.class);

		if (useInmapCombiner) {
			conf.setMapperClass(MapWithInMapperCombiningClass.class);
		} else {
			conf.setMapperClass(MapClass.class);
		}

		if (useCombiner) {
			conf.setCombinerClass(CombineClass.class);
		}

		if (useRange) {
			conf.setPartitionerClass(RangePartitioner.class);
		}

		conf.setReducerClass(ReduceClass.class);

		conf.setSpeculativeExecution(false);

		FileSystem.get(conf).delete(new Path(out), true);
		FileSystem.get(conf).delete(new Path(outm), true);

		JobClient.runJob(conf);

		float mass = Float.NEGATIVE_INFINITY;
		FileSystem fs = FileSystem.get(conf);
		for (FileStatus f : fs.listStatus(new Path(outm))) {
			FSDataInputStream fin = fs.open(f.getPath());
			mass = sumLogProbs(mass, fin.readFloat());
			fin.close();
		}

		return mass;
	}

	private void phase2(String path, int i, int j, int n, float missing) throws IOException {
		JobConf conf = new JobConf(RunPageRankBasic.class);

		sLogger.info("missing PageRank mass: " + missing);
		sLogger.info("number of nodes: " + n);

		String in = path + "/iter" + sFormat.format(j) + "t";
		String out = path + "/iter" + sFormat.format(j);

		sLogger.info("PageRank: iteration " + j + ": Phase2");
		sLogger.info(" - input: " + in);
		sLogger.info(" - output: " + out);

		int numMapTasks = FileSystem.get(conf).listStatus(new Path(in)).length;
		int numReduceTasks = 0;

		conf.setJobName("PageRank:Basic:iteration" + j + ":Phase2");
		conf.setInt("mapred.min.split.size", 1024 * 1024 * 1024);
		conf.setFloat("MissingMass", (float) missing);
		conf.setInt("NodeCount", n);

		conf.setNumMapTasks(numMapTasks);
		conf.setNumReduceTasks(numReduceTasks);

		FileInputFormat.setInputPaths(conf, new Path(in));
		FileOutputFormat.setOutputPath(conf, new Path(out));

		conf.setInputFormat(SequenceFileInputFormat.class);
		conf.setOutputFormat(SequenceFileOutputFormat.class);

		conf.setMapOutputKeyClass(IntWritable.class);
		conf.setMapOutputValueClass(PageRankNode.class);

		conf.setOutputKeyClass(IntWritable.class);
		conf.setOutputValueClass(PageRankNode.class);

		conf.setMapperClass(MapPageRankMassDistributionClass.class);
		conf.setCombinerClass(IdentityReducer.class);
		conf.setReducerClass(IdentityReducer.class);

		FileSystem.get(conf).delete(new Path(out), true);

		JobClient.runJob(conf);
	}

	// adds two log probs
	private static float sumLogProbs(float a, float b) {
		if (a == Float.NEGATIVE_INFINITY)
			return b;

		if (b == Float.NEGATIVE_INFINITY)
			return a;

		if (a < b) {
			return (float) (b + StrictMath.log1p(StrictMath.exp(a - b)));
		}

		return (float) (a + StrictMath.log1p(StrictMath.exp(b - a)));
	}
}
