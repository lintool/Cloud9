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

package edu.umd.cloud9.example.bigram;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

public class BigramCount extends Configured implements Tool {
  private static final Logger LOG = Logger.getLogger(BigramCount.class);

  // Mapper: emits (token, 1) for every bigram occurrence.
  protected static class MyMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    // Reuse objects to save overhead of object creation.
    private static final IntWritable one = new IntWritable(1);
    private static final Text bigram = new Text();

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException,
        InterruptedException {
      String line = value.toString();

      String prev = null;
      StringTokenizer itr = new StringTokenizer(line);
      while (itr.hasMoreTokens()) {
        String cur = itr.nextToken();

        // Emit only if we have an actual bigram.
        if (prev != null) {
          bigram.set(prev + " " + cur);
          context.write(bigram, one);
        }
        prev = cur;
      }
    }
  }

  // Reducer: sums up all the counts.
  protected static class MyReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    // Reuse objects
    private final static IntWritable sumWritable = new IntWritable();

    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException,
        InterruptedException {
      // sum up values
      int sum = 0;
      Iterator<IntWritable> iter = values.iterator();
      while (iter.hasNext()) {
        sum += iter.next().get();
      }
      sumWritable.set(sum);
      context.write(key, sumWritable);
    }
  }

  private BigramCount() {
  }

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
    int reduceTasks = Integer.parseInt(args[2]);

    LOG.info("Tool name: BigramCount");
    LOG.info(" - input path: " + inputPath);
    LOG.info(" - output path: " + outputPath);
    LOG.info(" - num reducers: " + reduceTasks);

    Job job = new Job(getConf(), "BigramCount");
    job.setJarByClass(BigramCount.class);

    job.setNumReduceTasks(reduceTasks);

    FileInputFormat.setInputPaths(job, new Path(inputPath));
    FileOutputFormat.setOutputPath(job, new Path(outputPath));

    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IntWritable.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    job.setOutputFormatClass(SequenceFileOutputFormat.class);

    job.setMapperClass(MyMapper.class);
    job.setCombinerClass(MyReducer.class);
    job.setReducerClass(MyReducer.class);

    // Delete the output directory if it exists already
    Path outputDir = new Path(outputPath);
    FileSystem.get(getConf()).delete(outputDir, true);

    long startTime = System.currentTimeMillis();
    job.waitForCompletion(true);
    System.out.println("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0
        + " seconds");

    return 0;
  }

  /**
   * Dispatches command-line arguments to the tool via the <code>ToolRunner</code>.
   */
  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new BigramCount(), args);
    System.exit(res);
  }
}
