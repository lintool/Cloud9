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

package edu.umd.cloud9.webgraph.driver;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import edu.umd.cloud9.io.array.ArrayListWritable;
import edu.umd.cloud9.webgraph.data.AnchorText;
import edu.umd.cloud9.webgraph.DriverUtil;


/**
 * <p>
 * Main driver program for generating a tab-delimited web graph. Each line in the output file
 * starts with a [node-id] and is followed by a list of [node-id]s all separated by tab characters.
 * The first part indicates a page in the web graph, and the rest are the pages which are directly
 * pointed to by that pages. Command-line arguments are as follows:
 * </p>
 *
 * <ul>
 * <li>[input-path]: the base path to the webgraph</li>
 * <li>[output-path]: the output path</li>
 * </ul>
 *
 * @author Nima Asadi
 *
 */


@SuppressWarnings("deprecation")
public class GenerateTabDelimitedWebGraph extends Configured implements Tool {
  private static class MyMapper extends MapReduceBase implements
  Mapper<IntWritable, ArrayListWritable<AnchorText>, IntWritable, Text> {
    private static final Text valueOutput = new Text();
    private static final StringBuilder buffer = new StringBuilder();

    public void map(IntWritable key, ArrayListWritable<AnchorText> anchors,
        OutputCollector<IntWritable, Text> output, Reporter reporter)
        throws IOException {
      buffer.delete(0, buffer.length());

      for(AnchorText p : anchors) {
        if(!p.isExternalOutLink() && !p.isInternalOutLink()) {
          continue;
        }
        for(int doc : p) {
          buffer.append(doc + "\t");
        }
      }

      valueOutput.set(buffer.toString());
      output.collect(key, valueOutput);
    }
  }

  private static int printUsage() {
    System.out.println("usage: -webgraph [WebGraph-base-path] -output [output-path]");
    ToolRunner.printGenericCommandUsage(System.out);
    return -1;
  }

  public int run(String[] args) throws Exception {
    if(args.length < 4) {
      printUsage();
      return -1;
    }

    JobConf conf = new JobConf(getConf(), GenerateTabDelimitedWebGraph.class);
    FileSystem fs = FileSystem.get(conf);

    String inPath = DriverUtil.argValue(args, "-webgraph") + "/" +
      DriverUtil.OUTPUT_WEBGRAPH;
    String outPath = DriverUtil.argValue(args, "-output");

    Path inputPath = new Path(inPath);
    Path outputPath = new Path(outPath);

    if (fs.exists(outputPath)) {
      fs.delete(outputPath);
    }

    conf.setJobName("TabDelimWebGraph");
    conf.set("mapred.child.java.opts", "-Xmx2048m");
    conf.set("mapreduce.map.memory.mb", "2048");
    conf.set("mapreduce.map.java.opts", "-Xmx2048m");
    conf.set("mapreduce.reduce.memory.mb", "2048");
    conf.set("mapreduce.reduce.java.opts", "-Xmx2048m");
    conf.set("mapreduce.task.timeout", "60000000");

    conf.setNumMapTasks(1);
    conf.setNumReduceTasks(0);

    FileInputFormat.setInputPaths(conf, inputPath);
    FileOutputFormat.setOutputPath(conf, outputPath);

    conf.setInputFormat(SequenceFileInputFormat.class);
    conf.setOutputFormat(TextOutputFormat.class);
    conf.setOutputKeyClass(IntWritable.class);
    conf.setOutputValueClass(Text.class);
    conf.setMapperClass(MyMapper.class);

    JobClient.runJob(conf);
    return 0;
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(),
        new GenerateTabDelimitedWebGraph(), args);
    System.exit(res);
  }
}
