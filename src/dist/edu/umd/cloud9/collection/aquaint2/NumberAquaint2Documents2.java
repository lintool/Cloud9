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

package edu.umd.cloud9.collection.aquaint2;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
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
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;


public class NumberAquaint2Documents2 extends Configured implements Tool {
  private static final Logger LOG = Logger.getLogger(NumberAquaint2Documents2.class);
  {
    LOG.setLevel(Level.INFO);
  }

  private static enum Count { DOCS };

  private static class MyMapper extends Mapper<LongWritable, Aquaint2Document, Text, IntWritable> {
    private static final Text docid = new Text();
    private static final IntWritable one = new IntWritable(1);

    @Override
    public void map(LongWritable key, Aquaint2Document doc, Context context)
        throws IOException, InterruptedException {
      context.getCounter(Count.DOCS).increment(1);
      docid.set(doc.getDocid());
      context.write(docid, one);
      LOG.setLevel(Level.INFO);
      LOG.trace("map output (" + docid + ", " + one + ")");
    }
  }

  private static class MyReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    private final static IntWritable cnt = new IntWritable(1);

    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context)
        throws IOException, InterruptedException {
      context.write(key, cnt);
      cnt.set(cnt.get() + 1);
      LOG.setLevel(Level.INFO);
      LOG.trace("reduce output (" + key + ", " + cnt + ")");
    }
  }

  /**
   * Creates an instance of this tool.
   */
  public NumberAquaint2Documents2() {}

  private static int printUsage() {
    System.out.println("usage: [input-path] [output-path] [output-file]");
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

    Path inputDirPath = new Path(args[0]);
    String outputDirPathname = args[1];
    Path outputDirPath = new Path(outputDirPathname);
    Path outputFilePath = new Path(args[2]);

    LOG.info("Tool: " + NumberAquaint2Documents2.class.getCanonicalName());
    LOG.info(" - Input dir path: " + inputDirPath);
    LOG.info(" - Output dir path: " + outputDirPath);
    LOG.info(" - Output file path: " + outputFilePath);

    Configuration conf = getConf();
    FileSystem fs = FileSystem.get(conf);

    Job job = new Job(conf, NumberAquaint2Documents2.class.getSimpleName());
    job.setJarByClass(NumberAquaint2Documents2.class);

    job.setNumReduceTasks(1);

    FileInputFormat.setInputPaths(job, inputDirPath);
    FileOutputFormat.setOutputPath(job, outputDirPath);
    FileOutputFormat.setCompressOutput(job, false);

    job.setInputFormatClass(Aquaint2DocumentInputFormat2.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    job.setOutputFormatClass(TextOutputFormat.class);

    job.setMapperClass(MyMapper.class);
    job.setReducerClass(MyReducer.class);

    // Delete the output directory if it exists already.
    fs.delete(outputDirPath, true);

    job.waitForCompletion(true);

    Path inputFilePath = new Path(outputDirPathname
                                  + (outputDirPathname.endsWith("/") ? "" : "/")
                                  + "/part-r-00000");
    Aquaint2DocnoMapping.writeDocnoData(inputFilePath, outputFilePath,
                                        FileSystem.get(getConf()));

    return 0;
  }

  /**
   * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
   */
  public static void main(String[] args) throws Exception {
    ToolRunner.run(new Configuration(), new NumberAquaint2Documents2(), args);
  }
}
