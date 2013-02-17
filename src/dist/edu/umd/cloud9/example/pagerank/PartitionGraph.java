/*
 * Cloud9: A Hadoop toolkit for working with big data
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

package edu.umd.cloud9.example.pagerank;

import java.util.Arrays;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import edu.umd.cloud9.mapreduce.lib.input.NonSplitableSequenceFileInputFormat;

/**
 * <p>Driver program for partitioning the graph.</p>
 *
 * @author Jimmy Lin
 * @author Michael Schatz
 */
public class PartitionGraph extends Configured implements Tool {
	private static final Logger LOG = Logger.getLogger(PartitionGraph.class);

	public static void main(String[] args) throws Exception {
		ToolRunner.run(new PartitionGraph(), args);
	}

	public PartitionGraph() {}

  private static final String INPUT = "input";
  private static final String OUTPUT = "output";
  private static final String NUM_NODES = "numNodes";
  private static final String NUM_PARTITIONS = "numPartitions";
  private static final String RANGE = "range";

  /**
   * Runs this tool.
   */
  @SuppressWarnings({ "static-access" })
  public int run(String[] args) throws Exception {
    Options options = new Options();

    options.addOption(new Option(RANGE, "use range partitioner"));

    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("input path").create(INPUT));
    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("output path").create(OUTPUT));
    options.addOption(OptionBuilder.withArgName("num").hasArg()
        .withDescription("number of nodes").create(NUM_NODES));
    options.addOption(OptionBuilder.withArgName("num").hasArg()
        .withDescription("number of partitions").create(NUM_PARTITIONS));

    CommandLine cmdline;
    CommandLineParser parser = new GnuParser();

    try {
      cmdline = parser.parse(options, args);
    } catch (ParseException exp) {
      System.err.println("Error parsing command line: " + exp.getMessage());
      return -1;
    }

    if (!cmdline.hasOption(INPUT) || !cmdline.hasOption(OUTPUT) ||
        !cmdline.hasOption(NUM_NODES) || !cmdline.hasOption(NUM_PARTITIONS)) {
      System.out.println("args: " + Arrays.toString(args));
      HelpFormatter formatter = new HelpFormatter();
      formatter.setWidth(120);
      formatter.printHelp(this.getClass().getName(), options);
      ToolRunner.printGenericCommandUsage(System.out);
      return -1;
    }

    String inPath = cmdline.getOptionValue(INPUT);
    String outPath = cmdline.getOptionValue(OUTPUT);
    int nodeCount = Integer.parseInt(cmdline.getOptionValue(NUM_NODES));
		int numParts = Integer.parseInt(cmdline.getOptionValue(NUM_PARTITIONS));
		boolean useRange = cmdline.hasOption(RANGE);

		LOG.info("Tool name: " + PartitionGraph.class.getSimpleName());
		LOG.info(" - input dir: " + inPath);
		LOG.info(" - output dir: " + outPath);
		LOG.info(" - num partitions: " + numParts);
		LOG.info(" - node cnt: " + nodeCount);
    LOG.info(" - use range partitioner: " + useRange);

		Configuration conf = getConf();
		conf.setInt("NodeCount", nodeCount);

		Job job = Job.getInstance(conf);
		job.setJobName(PartitionGraph.class.getSimpleName() + ":" + inPath);
		job.setJarByClass(PartitionGraph.class);

		job.setNumReduceTasks(numParts);

		FileInputFormat.setInputPaths(job, new Path(inPath));
		FileOutputFormat.setOutputPath(job, new Path(outPath));

		job.setInputFormatClass(NonSplitableSequenceFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(PageRankNode.class);

		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(PageRankNode.class);

		if (useRange) {
			job.setPartitionerClass(RangePartitioner.class);
		}

		FileSystem.get(conf).delete(new Path(outPath), true);

		job.waitForCompletion(true);

		return 0;
	}
}
