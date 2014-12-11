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
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

/**
 * Tool for extracting nodes that are a particular distance from the source node.
 *
 * @author Jimmy Lin
 */
public class FindNodeAtDistance extends Configured implements Tool {
  private static final Logger LOG = Logger.getLogger(FindNodeAtDistance.class);

  private static class MyMapper extends Mapper<IntWritable, BfsNode, IntWritable, BfsNode> {
    private static int distance;

    @Override
    public void setup(Context context) {
      distance = context.getConfiguration().getInt(DISTANCE_OPTION, 0);
    }

    @Override
    public void map(IntWritable nid, BfsNode node, Context context)
        throws IOException, InterruptedException {
      if (node.getDistance() == distance) {
        context.write(nid, node);
      }
    }
  }

  public FindNodeAtDistance() {}

  private static final String INPUT_OPTION = "input";
  private static final String OUTPUT_OPTION = "output";
  private static final String DISTANCE_OPTION = "distance";

  @SuppressWarnings("static-access")
  @Override
  public int run(String[] args) throws Exception {
    Options options = new Options();
    options.addOption(OptionBuilder.withArgName("path")
        .hasArg().withDescription("XML dump file").create(INPUT_OPTION));
    options.addOption(OptionBuilder.withArgName("path")
        .hasArg().withDescription("output path").create(OUTPUT_OPTION));
    options.addOption(OptionBuilder.withArgName("num")
        .hasArg().withDescription("distance").create(DISTANCE_OPTION));

    CommandLine cmdline;
    CommandLineParser parser = new GnuParser();
    try {
      cmdline = parser.parse(options, args);
    } catch (ParseException exp) {
      System.err.println("Error parsing command line: " + exp.getMessage());
      return -1;
    }

    if (!cmdline.hasOption(INPUT_OPTION) || !cmdline.hasOption(OUTPUT_OPTION)
        || !cmdline.hasOption(DISTANCE_OPTION)) {
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp(this.getClass().getName(), options);
      ToolRunner.printGenericCommandUsage(System.out);
      return -1;
    }

    String inputPath = cmdline.getOptionValue(INPUT_OPTION);
    String outputPath = cmdline.getOptionValue(OUTPUT_OPTION);
    int distance = Integer.parseInt(cmdline.getOptionValue(DISTANCE_OPTION));

    LOG.info("Tool name: " + this.getClass().getName());
    LOG.info(" - inputDir: " + inputPath);
    LOG.info(" - outputDir: " + outputPath);
    LOG.info(" - distance: " + distance);

    Job job = Job.getInstance(getConf());
    job.setJobName(String.format("FindNodeAtDistance[%s: %s, %s: %s, %s: %d]",
        INPUT_OPTION, inputPath, OUTPUT_OPTION, outputPath, DISTANCE_OPTION, distance));
    job.setJarByClass(FindNodeAtDistance.class);

    job.setNumReduceTasks(0);

    job.getConfiguration().setInt(DISTANCE_OPTION, distance);
    job.getConfiguration().setInt("mapred.min.split.size", 1024 * 1024 * 1024);

    FileInputFormat.addInputPath(job, new Path(inputPath));
    FileOutputFormat.setOutputPath(job, new Path(outputPath));

    job.setInputFormatClass(SequenceFileInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);

    job.setMapOutputKeyClass(IntWritable.class);
    job.setMapOutputValueClass(BfsNode.class);
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(BfsNode.class);

    job.setMapperClass(MyMapper.class);

    // Delete the output directory if it exists already.
    FileSystem.get(job.getConfiguration()).delete(new Path(outputPath), true);

    job.waitForCompletion(true);

    return 0;
  }

  /**
   * Dispatches command-line arguments to the tool via the <code>ToolRunner</code>.
   */
  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new FindNodeAtDistance(), args);
    System.exit(res);
  }
}
