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

package edu.umd.cloud9.example.simple;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
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
import org.apache.pig.data.Tuple;

import edu.umd.cloud9.io.pair.PairOfStringInt;

/**
 * Modified word count demo designed to work with {@link DemoPackTuples1}. Counts words on
 * even-length or odd-length lines to demonstrate use of specialized intermediate data structures.
 *
 * @author Jimmy Lin
 */
public class DemoWordCountTuple1 extends Configured implements Tool {
  private static final Logger LOG = Logger.getLogger(DemoWordCountTuple1.class);

  // Mapper that emits tuple as the key, and value '1' for each occurrence.
  private static class MyMapper extends Mapper<LongWritable, Tuple, PairOfStringInt, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private final static PairOfStringInt PAIR = new PairOfStringInt();

    @Override
    public void map(LongWritable key, Tuple tuple, Context context) throws IOException,
        InterruptedException {
      String line = (String) tuple.get(0);
      StringTokenizer itr = new StringTokenizer(line);
      while (itr.hasMoreTokens()) {
        String token = itr.nextToken();

        PAIR.set(token, line.length() % 2);

        context.write(PAIR, one);
      }
    }
  }

  // Reducer counts up tuple occurrences.
  private static class MyReducer extends
      Reducer<PairOfStringInt, IntWritable, PairOfStringInt, IntWritable> {
    private final static IntWritable SUM = new IntWritable();

    @Override
    public void reduce(PairOfStringInt tupleKey, Iterable<IntWritable> values, Context context)
        throws IOException, InterruptedException {
      Iterator<IntWritable> iter = values.iterator();
      int sum = 0;
      while (iter.hasNext()) {
        sum += iter.next().get();
      }

      // Keep original tuple key, emit sum of counts as value.
      SUM.set(sum);
      context.write(tupleKey, SUM);
    }
  }

  /**
   * Creates an instance of this tool.
   */
  public DemoWordCountTuple1() {}

  private static int printUsage() {
    System.out.println("usage: [input-path] [output-path] [num-reducers]");
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
    int numReduceTasks = Integer.parseInt(args[2]);

    LOG.info("Tool: " + DemoWordCountTuple1.class.getSimpleName());
    LOG.info(" - input path: " + inputPath);
    LOG.info(" - output path: " + outputPath);
    LOG.info(" - number of reducers: " + numReduceTasks);

    Configuration conf = getConf();
    Job job = Job.getInstance(conf);
    job.setJobName(DemoWordCountTuple1.class.getSimpleName());
    job.setJarByClass(DemoWordCountTuple1.class);
    job.setNumReduceTasks(numReduceTasks);

    FileInputFormat.setInputPaths(job, new Path(inputPath));
    FileOutputFormat.setOutputPath(job, new Path(outputPath));

    job.setInputFormatClass(SequenceFileInputFormat.class);
    job.setOutputFormatClass(SequenceFileOutputFormat.class);
    job.setOutputKeyClass(PairOfStringInt.class);
    job.setOutputValueClass(IntWritable.class);

    job.setMapperClass(MyMapper.class);
    job.setCombinerClass(MyReducer.class);
    job.setReducerClass(MyReducer.class);

    // Delete the output directory if it exists already.
    Path outputDir = new Path(outputPath);
    FileSystem.get(conf).delete(outputDir, true);

    long startTime = System.currentTimeMillis();
    job.waitForCompletion(true);
    LOG.info("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");

    return 0;
  }

  /**
   * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
   */
  public static void main(String[] args) throws Exception {
    ToolRunner.run(new DemoWordCountTuple1(), args);
  }
}
