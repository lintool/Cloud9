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

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
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

import edu.umd.cloud9.io.array.ArrayListOfIntsWritable;
import edu.umd.cloud9.util.array.ArrayListOfInts;
import edu.umd.cloud9.util.map.HMapII;
import edu.umd.cloud9.util.map.MapII;

/**
 * Tool for running one iteration of parallel breadth-first search.
 *
 * @author Jimmy Lin
 */
public class IterateBFS extends Configured implements Tool {
	private static final Logger LOG = Logger.getLogger(IterateBFS.class);

	private static enum ReachableNodes {
		Map, Reduce
	};

	// Mapper with in-mapper combiner optimization.
	private static class MapClass extends	Mapper<IntWritable, BFSNode, IntWritable, BFSNode> {
		// For buffering distances keyed by destination node.
		private static final HMapII map = new HMapII();

		// For passing along node structure.
		private static final BFSNode intermediateStructure = new BFSNode();

		@Override
		public void map(IntWritable nid, BFSNode node, Context context)
		    throws IOException, InterruptedException {
			// Pass along node structure.
			intermediateStructure.setNodeId(node.getNodeId());
			intermediateStructure.setType(BFSNode.Type.Structure);
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
				dist.setType(BFSNode.Type.Distance);
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

				if (n.getType() == BFSNode.Type.Structure) {
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

			node.setType(BFSNode.Type.Complete);
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

	public IterateBFS() {}

  private static final String INPUT_OPTION = "input";
  private static final String OUTPUT_OPTION = "output";
  private static final String NUM_PARTITIONS_OPTION = "num_partitions";

  @SuppressWarnings("static-access") @Override
  public int run(String[] args) throws Exception {
    Options options = new Options();
    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("XML dump file").create(INPUT_OPTION));
    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("output path").create(OUTPUT_OPTION));
    options.addOption(OptionBuilder.withArgName("num").hasArg()
        .withDescription("number of partitions").create(NUM_PARTITIONS_OPTION));

    CommandLine cmdline;
    CommandLineParser parser = new GnuParser();
    try {
      cmdline = parser.parse(options, args);
    } catch (ParseException exp) {
      System.err.println("Error parsing command line: " + exp.getMessage());
      return -1;
    }

    if (!cmdline.hasOption(INPUT_OPTION) || !cmdline.hasOption(OUTPUT_OPTION) ||
        !cmdline.hasOption(NUM_PARTITIONS_OPTION)) {
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp(this.getClass().getName(), options);
      ToolRunner.printGenericCommandUsage(System.out);
      return -1;
    }

    String inputPath = cmdline.getOptionValue(INPUT_OPTION);
    String outputPath = cmdline.getOptionValue(OUTPUT_OPTION);
    int n = Integer.parseInt(cmdline.getOptionValue(NUM_PARTITIONS_OPTION));

    LOG.info("Tool name: " + this.getClass().getName());
		LOG.info(" - inputDir: " + inputPath);
		LOG.info(" - outputDir: " + outputPath);
		LOG.info(" - numPartitions: " + n);

		getConf().set("mapred.child.java.opts", "-Xmx2048m");

		Job job = new Job(getConf(), String.format("IterateBFS[%s: %s, %s: %s, %s: %d]",
        INPUT_OPTION, inputPath, OUTPUT_OPTION, outputPath, NUM_PARTITIONS_OPTION, n));
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