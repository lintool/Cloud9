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
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import edu.umd.cloud9.io.array.ArrayListOfIntsWritable;

/**
 * Tool for taking a plain-text encoding of a directed graph and building
 * corresponding Hadoop structures for running parallel breadth-first search.
 *
 * @author Jimmy Lin
 */
public class EncodeBFSGraph extends Configured implements Tool {
	private static final Logger LOG = Logger.getLogger(EncodeBFSGraph.class);

	private static enum Graph {
		Nodes, Edges
	};

	private static class MyMapper extends Mapper<LongWritable, Text, IntWritable, BFSNode> {
		private static final IntWritable nid = new IntWritable();
		private static final BFSNode node = new BFSNode();
		private static int src;

		@Override
		public void setup(Context context) {
			src = context.getConfiguration().getInt(SRC_OPTION, 0);
			node.setType(BFSNode.Type.Complete);
		}

		@Override
		public void map(LongWritable key, Text t, Context context)
		    throws IOException,	InterruptedException {
			String[] arr = t.toString().trim().split("\\s+");

			int cur = Integer.parseInt(arr[0]);
			nid.set(cur);
			node.setNodeId(cur);
			node.setDistance(cur == src ? 0 : Integer.MAX_VALUE);

			if (arr.length == 1) {
				node.setAdjacencyList(new ArrayListOfIntsWritable());
			} else {
				int[] neighbors = new int[arr.length - 1];
				for (int i = 1; i < arr.length; i++) {
					neighbors[i - 1] = Integer.parseInt(arr[i]);
				}
				node.setAdjacencyList(new ArrayListOfIntsWritable(neighbors));
			}

			context.getCounter(Graph.Nodes).increment(1);
			context.getCounter(Graph.Edges).increment(arr.length - 1);

			context.write(nid, node);
		}
	}

	public EncodeBFSGraph() {}

  private static final String INPUT_OPTION = "input";
  private static final String OUTPUT_OPTION = "output";
  private static final String SRC_OPTION = "src";

  @SuppressWarnings("static-access") @Override
  public int run(String[] args) throws Exception {
    Options options = new Options();
    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("XML dump file").create(INPUT_OPTION));
    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("output path").create(OUTPUT_OPTION));
    options.addOption(OptionBuilder.withArgName("nodeid").hasArg()
        .withDescription("source node").create(SRC_OPTION));

    CommandLine cmdline;
    CommandLineParser parser = new GnuParser();
    try {
      cmdline = parser.parse(options, args);
    } catch (ParseException exp) {
      System.err.println("Error parsing command line: " + exp.getMessage());
      return -1;
    }

    if (!cmdline.hasOption(INPUT_OPTION) || !cmdline.hasOption(OUTPUT_OPTION) ||
        !cmdline.hasOption(SRC_OPTION)) {
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp(this.getClass().getName(), options);
      ToolRunner.printGenericCommandUsage(System.out);
      return -1;
    }

		String inputPath = cmdline.getOptionValue(INPUT_OPTION);
		String outputPath = cmdline.getOptionValue(OUTPUT_OPTION);
		int src = Integer.parseInt(cmdline.getOptionValue(SRC_OPTION));

		LOG.info("Tool name: " + this.getClass().getName());
		LOG.info(" - inputDir: " + inputPath);
		LOG.info(" - outputDir: " + outputPath);
		LOG.info(" - src: " + src);

		Job job = new Job(getConf(), String.format("EncodeBFSGraph[%s: %s, %s: %s, %s: %d]",
		    INPUT_OPTION, inputPath, OUTPUT_OPTION, outputPath, SRC_OPTION, src));
		job.setJarByClass(EncodeBFSGraph.class);

		job.setNumReduceTasks(0);

		job.getConfiguration().setInt(SRC_OPTION, src);
		job.getConfiguration().setInt("mapred.min.split.size", 1024 * 1024 * 1024);

		FileInputFormat.addInputPath(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(BFSNode.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(BFSNode.class);

		job.setMapperClass(MyMapper.class);

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
		int res = ToolRunner.run(new EncodeBFSGraph(), args);
		System.exit(res);
	}
}
