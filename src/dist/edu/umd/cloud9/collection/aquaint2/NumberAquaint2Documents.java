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
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

@SuppressWarnings("deprecation")
public class NumberAquaint2Documents extends Configured implements Tool {
  private static final Logger LOG = Logger.getLogger(NumberAquaint2Documents.class);
  private static enum Count { DOCS };

  private static class MyMapper extends MapReduceBase implements
      Mapper<LongWritable, Aquaint2Document, Text, IntWritable> {
    private final static Text text = new Text();
    private final static IntWritable count = new IntWritable(1);

    public void map(LongWritable key, Aquaint2Document doc,
        OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
      reporter.incrCounter(Count.DOCS, 1);
      text.set(doc.getDocid());
      output.collect(text, count);
    }
  }

  private static class MyReducer extends MapReduceBase implements
      Reducer<Text, IntWritable, Text, IntWritable> {
    private final static IntWritable count = new IntWritable(1);

    public void reduce(Text key, Iterator<IntWritable> values,
        OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
      output.collect(key, count);
      count.set(count.get() + 1);
    }
  }

  /**
   * Creates an instance of this tool.
   */
  public NumberAquaint2Documents() {}

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

    String inputPath = args[0];
    String outputPath = args[1];
    String outputFile = args[2];
    int mapTasks = 10;

    LOG.info("Tool: " + NumberAquaint2Documents.class.getCanonicalName());
    LOG.info(" - Input path: " + inputPath);
    LOG.info(" - Output path: " + outputPath);
    LOG.info(" - Output file: " + outputFile);

    JobConf conf = new JobConf(NumberAquaint2Documents.class);
    conf.setJobName(NumberAquaint2Documents.class.getSimpleName());

    conf.setNumMapTasks(mapTasks);
    conf.setNumReduceTasks(1);

    FileInputFormat.setInputPaths(conf, new Path(inputPath));
    FileOutputFormat.setOutputPath(conf, new Path(outputPath));
    FileOutputFormat.setCompressOutput(conf, false);

    conf.setInputFormat(Aquaint2DocumentInputFormat.class);
    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(IntWritable.class);
    conf.setOutputFormat(TextOutputFormat.class);

    conf.setMapperClass(MyMapper.class);
    conf.setReducerClass(MyReducer.class);

    // Delete the output directory if it exists already.
    FileSystem.get(conf).delete(new Path(outputPath), true);

    JobClient.runJob(conf);

    Aquaint2DocnoMapping.writeDocnoData(new Path(outputPath + "/part-00000"),
        new Path(outputFile), FileSystem.get(conf));

    return 0;
  }

  /**
   * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
   */
  public static void main(String[] args) throws Exception {
    ToolRunner.run(new Configuration(), new NumberAquaint2Documents(), args);
  }
}
