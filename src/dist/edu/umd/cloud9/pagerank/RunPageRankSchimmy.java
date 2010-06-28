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
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Partitioner;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.lib.HashPartitioner;
import org.apache.hadoop.mapred.lib.IdentityReducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import edu.umd.cloud9.io.ArrayListOfIntsWritable;
import edu.umd.cloud9.util.HMapIF;
import edu.umd.cloud9.util.MapIF;

/**
 * <p>
 * Main driver program for running the Schimmy implementation of PageRank.
 * Command-line arguments are as follows:
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
 * @see RunPageRankBasic
 * @author Jimmy Lin
 * @author Michael Schatz
 * 
 */
public class RunPageRankSchimmy extends Configured implements Tool {

	private static final Logger sLogger = Logger.getLogger(RunPageRankSchimmy.class);

	// mapper, no in-mapper combining
	private static class MapClass extends MapReduceBase implements
			Mapper<IntWritable, PageRankNode, IntWritable, FloatWritable> {

		// the neighbor to which we're sending messages
		private static IntWritable sNeighbor = new IntWritable();

		// contents of the messages: partial PageRank mass
		private static FloatWritable sIntermediateMass = new FloatWritable();

		public void map(IntWritable nid, PageRankNode node,
				OutputCollector<IntWritable, FloatWritable> output, Reporter reporter)
				throws IOException {

			int massMessages = 0;

			// distribute PageRank mass to neighbors (along outgoing edges)
			if (node.getAdjacenyList().size() > 0) {
				// each neighbor gets an equal share of PageRank mass
				ArrayListOfIntsWritable list = node.getAdjacenyList();
				float mass = node.getPageRank() - (float) StrictMath.log(list.size());

				// iterate over neighbors
				for (int i = 0; i < list.size(); i++) {
					sNeighbor.set(list.get(i));
					sIntermediateMass.set(mass);

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
			Mapper<IntWritable, PageRankNode, IntWritable, FloatWritable> {

		// save a reference to the output collector
		private static OutputCollector<IntWritable, FloatWritable> mOutput;

		// for buffering PageRank mass contributes keyed by destination node
		private static HMapIF map = new HMapIF();

		public void map(IntWritable nid, PageRankNode node,
				OutputCollector<IntWritable, FloatWritable> output, Reporter reporter)
				throws IOException {
			mOutput = output;

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
			FloatWritable v = new FloatWritable();

			for (MapIF.Entry e : map.entrySet()) {
				k.set(e.getKey());
				v.set(e.getValue());

				mOutput.collect(k, v);
			}
		}
	}

	// combiner: sums partial PageRank contributions
	private static class CombineClass extends MapReduceBase implements
			Reducer<IntWritable, FloatWritable, IntWritable, FloatWritable> {

		private static FloatWritable sIntermediateMass = new FloatWritable();

		public void reduce(IntWritable nid, Iterator<FloatWritable> values,
				OutputCollector<IntWritable, FloatWritable> output, Reporter reporter)
				throws IOException {

			int massMessages = 0;

			// remember, PageRank mass is stored as a log prob
			float mass = Float.NEGATIVE_INFINITY;
			while (values.hasNext()) {
				// accumulate PageRank mass contributions
				FloatWritable n = values.next();
				mass = sumLogProbs(mass, n.get());

				massMessages++;
			}

			// emit aggregated results
			if (massMessages > 0) {
				sIntermediateMass.set(mass);
				output.collect(nid, sIntermediateMass);
			}
		}
	}

	// reduce: sums incoming PageRank contributions, rewrite graph structure
	private static class ReduceClass extends MapReduceBase implements
			Reducer<IntWritable, FloatWritable, IntWritable, PageRankNode> {

		private JobConf mJobConf;
		private String mTaskId;
		private String mPath;

		private OutputCollector<IntWritable, PageRankNode> mOutput;
		private Reporter mReporter;

		private float mTotalMass = Float.NEGATIVE_INFINITY;

		private SequenceFile.Reader reader;

		private IntWritable mStateNid = new IntWritable();
		private PageRankNode mStateNode = new PageRankNode();

		static {
			sLogger.setLevel(Level.INFO);
		}

		public void configure(JobConf job) {
			mJobConf = job;
			mTaskId = job.get("mapred.task.id");
			mPath = job.get("PageRankMassPath");

			// we want to reconstruct the mapping from partition file stored on
			// disk and the actual partition...
			String pMappingString = job.get("PartitionMapping");

			Map<Integer, String> m = new HashMap<Integer, String>();
			for (String s : pMappingString.split("\\t")) {
				String[] arr = s.split("=");

				sLogger.info(arr[0] + "\t" + arr[1]);

				m.put(Integer.parseInt(arr[0]), arr[1]);
			}

			int partno = Integer.parseInt(mTaskId.substring(mTaskId.length() - 7,
					mTaskId.length() - 2));
			String f = m.get(partno);

			sLogger.info("task id: " + mTaskId);
			sLogger.info("partno: " + partno);
			sLogger.info("file: " + f);

			try {
				FileSystem fs = FileSystem.get(job);
				reader = new SequenceFile.Reader(fs, new Path(f), job);
			} catch (IOException e) {
				e.printStackTrace();
				throw new RuntimeException("Couldn't open + " + f + " for partno: " + partno
						+ " within: " + mTaskId);
			}
		}

		public void reduce(IntWritable nid, Iterator<FloatWritable> values,
				OutputCollector<IntWritable, PageRankNode> output, Reporter reporter)
				throws IOException {
			mOutput = output;
			mReporter = reporter;

			// we're going to read the node structure until we get to the node
			// of the current message we're processing...
			while (reader.next(mStateNid, mStateNode)) {
				if (mStateNid.get() == nid.get())
					break;

				// nodes are sorted in each partition, so if we come across a
				// larger nid than the current message we're processing, there's
				// something seriously wrong...
				if (mStateNid.get() > nid.get()) {
					Partitioner<WritableComparable, Writable> p = new HashPartitioner<WritableComparable, Writable>();

					int sp = p.getPartition(mStateNid, mStateNode, mJobConf.getNumReduceTasks());
					int kp = p.getPartition(nid, mStateNode, mJobConf.getNumReduceTasks());

					throw new RuntimeException("Unexpected Schimmy failure during merge! nids: "
							+ mStateNid.get() + " " + nid.get() + " parts: " + sp + " " + kp);
				}

				mStateNode.setPageRank(Float.NEGATIVE_INFINITY);

				output.collect(mStateNid, mStateNode);
			}

			int massMessagesReceived = 0;
			float mass = Float.NEGATIVE_INFINITY;

			// now we process the messages...
			while (values.hasNext()) {
				float n = values.next().get();
				massMessagesReceived++;

				mass = sumLogProbs(mass, n);
				mTotalMass = sumLogProbs(mTotalMass, n);
			}

			// populate the node structure with the updated PageRank value
			mStateNode.setPageRank(mass);

			// write back to disk
			output.collect(nid, mStateNode);
			reporter.incrCounter("PageRank", "massMessagesReceived", massMessagesReceived);
		}

		public void close() throws IOException {
			FileSystem fs = FileSystem.get(mJobConf);
			Path path = new Path(mPath + "/" + mTaskId);
			FSDataOutputStream out = fs.create(path, false);
			out.writeFloat(mTotalMass);
			out.close();

			// we have to write out the rest of the nodes we haven't finished
			// reading yet (i.e., these are the ones who don't have any messages
			// sent to them)
			while (reader.next(mStateNid, mStateNode)) {
				mStateNode.setPageRank(Float.NEGATIVE_INFINITY);
				mOutput.collect(mStateNid, mStateNode);
			}

			reader.close();
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
		int res = ToolRunner.run(new Configuration(), new RunPageRankSchimmy(), args);
		System.exit(res);
	}

	public RunPageRankSchimmy() {
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
			System.err.println("Invalid number of args: " + args.length);
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

		FileSystem fs = FileSystem.get(conf);

		
		// we need to actually count the number of part files to get the number
		// of partitions (because the directory might contain _log)
		int numPartitions = 0;
		for (FileStatus s : FileSystem.get(conf).listStatus(new Path(in))) {
			if (s.getPath().getName().contains("part-"))
				numPartitions++;
		}
		
		conf.setInt("NodeCount", n);

		Partitioner p = null;

		if (useRange) {
			p = new RangePartitioner<IntWritable, Writable>();
			p.configure(conf);
		} else {
			p = new HashPartitioner<WritableComparable, Writable>();
		}

		// this is really annoying: the mapping between the partition numbers on
		// disk (i.e., part-XXXX) and what partition the file contains (i.e.,
		// key.hash % #reducer) is arbitrary... so this means that we need to
		// open up each partition, peek inside to find out.
		IntWritable key = new IntWritable();
		PageRankNode value = new PageRankNode();
		FileStatus[] status = fs.listStatus(new Path(in));

		StringBuilder sb = new StringBuilder();

		for (FileStatus f : status) {
			if (f.getPath().getName().contains("_logs"))
				continue;

			SequenceFile.Reader reader = new SequenceFile.Reader(fs, f.getPath(), conf);

			reader.next(key, value);
			int np = p.getPartition(key, value, numPartitions);
			reader.close();

			sLogger.info(f.getPath() + "\t" + np);
			sb.append(np + "=" + f.getPath() + "\t");
		}

		sLogger.info(sb.toString().trim());

		sLogger.info("PageRankSchimmy: iteration " + j + ": Phase1");
		sLogger.info(" - input: " + in);
		sLogger.info(" - output: " + out);
		sLogger.info(" - nodeCnt: " + n);
		sLogger.info(" - useCombiner: " + useCombiner);
		sLogger.info(" - useInmapCombiner: " + useInmapCombiner);
		sLogger.info(" - numPartitions: " + numPartitions);
		sLogger.info(" - useRange: " + useRange);
		sLogger.info("computed number of partitions: " + numPartitions);

		int numMapTasks = numPartitions;
		int numReduceTasks = numPartitions;

		conf.setJobName("PageRankSchimmy:iteration" + j + ":Phase1");

		conf.setNumMapTasks(numMapTasks);
		conf.setNumReduceTasks(numReduceTasks);

		conf.setInt("mapred.min.split.size", 1024 * 1024 * 1024);
		conf.set("mapred.child.java.opts", "-Xmx2048m");

		conf.set("PageRankMassPath", outm);
		conf.set("BasePath", in);
		conf.set("PartitionMapping", sb.toString().trim());

		FileInputFormat.setInputPaths(conf, new Path(in));
		FileOutputFormat.setOutputPath(conf, new Path(out));

		conf.setInputFormat(SequenceFileInputFormat.class);
		conf.setOutputFormat(SequenceFileOutputFormat.class);

		conf.setMapOutputKeyClass(IntWritable.class);
		conf.setMapOutputValueClass(FloatWritable.class);

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

		sLogger.info("PageRankSchimmy: iteration " + j + ": Phase2");
		sLogger.info(" - input: " + in);
		sLogger.info(" - output: " + out);

		int numMapTasks = FileSystem.get(conf).listStatus(new Path(in)).length;
		int numReduceTasks = 0;

		conf.setJobName("PageRankSchimmy:iteration" + j + ":Phase2");
		conf.setInt("mapred.min.split.size", 1024 * 1024 * 1024);

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

		conf.setFloat("MissingMass", (float) missing);
		conf.setInt("NodeCount", n);

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
