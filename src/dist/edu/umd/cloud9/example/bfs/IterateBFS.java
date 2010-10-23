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

package edu.umd.cloud9.example.bfs;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import edu.umd.cloud9.io.ArrayListOfIntsWritable;
import edu.umd.cloud9.util.ArrayListOfInts;
import edu.umd.cloud9.util.HMapII;
import edu.umd.cloud9.util.MapII;

/**
 * <p>
 * Tool for running one iteration of parallel breadth-first search.
 * </p>
 *
 * @author Jimmy Lin
 *
 */
public class IterateBFS extends Configured implements Tool {

	private static final Logger LOG = Logger.getLogger(IterateBFS.class);

	private static enum ReachableNodes {
		Map, Reduce
	};

	// Mapper with in-mapper combiner optimization.
	private static class MapClass extends
			Mapper<IntWritable, BFSNode, IntWritable, BFSNode> {

		// For buffering distances keyed by destination node.
		private static final HMapII map = new HMapII();

		// For passing along node structure.
		private static final BFSNode intermediateStructure = new BFSNode();

		@Override
		public void map(IntWritable nid, BFSNode node, Context context) throws IOException,
				InterruptedException {

			// Pass along node structure.
			intermediateStructure.setNodeId(node.getNodeId());
			intermediateStructure.setType(BFSNode.TYPE_STRUCTURE);
			intermediateStructure.setAdjacencyList(node.getAdjacenyList());

			context.write(nid, intermediateStructure);

			if (node.getDistance() == Integer.MAX_VALUE) {
				return;
			}

			context.getCounter(ReachableNodes.Map).increment(1);
			// Retain distance to self.
			map.put(nid.get(), node.getDistance());

			ArrayListOfInts adj = node.getAdjacenyList();
			int dist = node.getDistance() + 1;
			// Keep track of shortest distance to neighbors.
			for (int i = 0; i < adj.size(); i++) {
				int neighbor = adj.get(i);

				// Keep track of distance if it's shorter than previously
				// encountered, or if we haven't encountered this node.
				if ((map.containsKey(neighbor) && dist < map.get(neighbor)) ||
						!map.containsKey(neighbor)) {
					map.put(neighbor, dist);
				}
			}
		}

		@Override
		public void cleanup(Mapper<IntWritable, BFSNode, IntWritable, BFSNode>.Context context)
				throws IOException, InterruptedException {
			// Now emit the messages all at once.
			IntWritable k = new IntWritable();
			BFSNode dist = new BFSNode();

			for (MapII.Entry e : map.entrySet()) {
				k.set(e.getKey());

				dist.setNodeId(e.getKey());
				dist.setType(BFSNode.TYPE_DISTANCE);
				dist.setDistance(e.getValue());

				context.write(k, dist);
			}
		}
	}

	// Reduce: sums incoming PageRank contributions, rewrite graph structure.
	private static class ReduceClass extends Reducer<IntWritable, BFSNode, IntWritable, BFSNode> {

		private static final BFSNode node = new BFSNode();

		@Override
		public void reduce(IntWritable nid, Iterable<BFSNode> iterable, Context context)
				throws IOException, InterruptedException {

			Iterator<BFSNode> values = iterable.iterator();

			int structureReceived = 0;
			int dist = Integer.MAX_VALUE;
			while (values.hasNext()) {
				BFSNode n = values.next();

				if (n.getType() == BFSNode.TYPE_STRUCTURE) {
					// This is the structure; update accordingly.
					ArrayListOfIntsWritable list = n.getAdjacenyList();
					structureReceived++;

					int arr[] = new int[list.size()];
					for (int i = 0; i < list.size(); i++) {
						arr[i] = list.get(i);
					}

					node.setAdjacencyList(new ArrayListOfIntsWritable(arr));
				} else {
					// This is a message that contains distance.
					if (n.getDistance() < dist) {
						dist = n.getDistance();
					}
				}
			}

			node.setType(BFSNode.TYPE_COMPLETE);
			node.setNodeId(nid.get());
			node.setDistance(dist); // Update the final distance.

			if (dist != Integer.MAX_VALUE) {
				context.getCounter(ReachableNodes.Reduce).increment(1);
			}

			// Error checking.
			if (structureReceived == 1) {
				// Everything checks out, emit final node structure with updated
				// distance.
				context.write(nid, node);
			} else if (structureReceived == 0) {
				// We get into this situation if there exists an edge pointing
				// to a node which has no corresponding node structure (i.e.,
				// distance was passed to a non-existent node)... log but move
				// on.
				LOG.warn("No structure received for nodeid: " + nid.get());
			} else {
				// This shouldn't happen!
				throw new RuntimeException("Multiple structure received for nodeid: " + nid.get()
						+ " struct: " + structureReceived);
			}
		}
	}

	public IterateBFS() {
	}

	private static int printUsage() {
		System.out.println("usage: [inputDir] [outputDir] [numPartitions]");
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

		LOG.info("Tool name: IterateBFS");
		LOG.info(" - inputDir: " + inputPath);
		LOG.info(" - outputDir: " + outputPath);
		LOG.info(" - numPartitions: " + n);

		Job job = new Job(getConf(), "IterateBFS");
		job.setJarByClass(EncodeBFSGraph.class);

		job.setNumReduceTasks(n);

		FileInputFormat.addInputPath(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));

		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(BFSNode.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(BFSNode.class);

		job.setMapperClass(MapClass.class);
		job.setReducerClass(ReduceClass.class);

		// Delete the output directory if it exists already.
		FileSystem.get(job.getConfiguration()).delete(new Path(outputPath), true);

		job.waitForCompletion(true);

		return 0;
	}

	/**
	 * Dispatches command-line arguments to the tool via the
	 * <code>ToolRunner</code>.
	 */
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new IterateBFS(), args);
		System.exit(res);
	}
}