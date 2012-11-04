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
import org.apache.hadoop.io.WritableComparable;
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

import edu.umd.cloud9.io.JsonWritable;

/**
 * <p>
 * Demo that illustrates use of {@link JSONObjectWritable} objects as intermediate keys in a
 * MapReduce job. This Hadoop Tool takes the following command-line arguments:
 * </p>
 * 
 * <ul>
 * <li>[input-path] input path</li>
 * <li>[output-path] output path</li>
 * <li>[num-reducers] number of reducers</li>
 * </ul>
 * 
 * <p>
 * Input comes from a flat text collection packed into a SequenceFile with {@link DemoPackJsonx}.
 * Output shows the count of words on even- and odd-length lines.
 * </p>
 * 
 * <p>
 * Format of the output SequenceFile: The key is a JSON object. The field named "Token" contains a
 * word and the field named "EvenOrOdd" indicates whether the word was found on a even-length or
 * odd-length line. The value is the count of the word on either even- or odd-length lines.
 * </p>
 * 
 * @see DemoWordCountTuple1
 * @see DemoWordCountTuple2
 * 
 * @author Jimmy Lin
 */
public class DemoWordCountJsonx extends Configured implements Tool {
  private static final Logger sLogger = Logger.getLogger(DemoWordCountJsonx.class);

  // define custom intermediate key; must specify sort order
  public static class MyKey extends JsonWritable implements WritableComparable<MyKey> {
    public int compareTo(MyKey that) {
      String thisToken = this.getJsonObject().get("Token").getAsString();
      String thatToken = that.getJsonObject().get("Token").getAsString();

      // if tokens are equal, must check "EvenOrOdd" field
      if (thisToken.equals(thatToken)) {
        // otherwise, sort by "EvenOrOdd" field
        int thisEO = this.getJsonObject().get("EvenOrOdd").getAsInt();
        int thatEO = that.getJsonObject().get("EvenOrOdd").getAsInt();

        if (thisEO < thatEO)
          return -1;

        if (thisEO > thatEO)
          return 1;

        // if we get here, it means the tuples are equal
        return 0;
      }

      // determine sort order based on token
      return thisToken.compareTo(thatToken);
    }

    public int hashCode() {
      return this.getJsonObject().getAsJsonPrimitive("Token").getAsString().hashCode();
    }
  }

  // mapper that emits a json object as the key, and value '1' for each
  // occurrence
  protected static class MyMapper extends Mapper<LongWritable, JsonWritable, MyKey, IntWritable> {

    // define value '1' statically so we can reuse the object, i.e., avoid
    // unnecessary object creation
    private final static IntWritable one = new IntWritable(1);

    // once again, reuse keys if possible
    private final static MyKey key = new MyKey();

    @Override
    public void map(LongWritable dummy, JsonWritable jsonIn, Context context)
        throws IOException, InterruptedException {

        // the input value is a JSON object
        String line = (String) jsonIn.getJsonObject().get("text").getAsString();
        StringTokenizer itr = new StringTokenizer(line);
        while (itr.hasMoreTokens()) {
          String token = itr.nextToken();

          // put new values into the tuple
          key.getJsonObject().addProperty("Token", token);
          key.getJsonObject().addProperty("EvenOrOdd", line.length() % 2);

          // emit key-value pair
          context.write(key, one);
        }
    }
  }

  // reducer counts up tuple occurrences
  protected static class MyReducer extends Reducer<MyKey, IntWritable, MyKey, IntWritable> {
    private final static IntWritable SumValue = new IntWritable();

    @Override
    public void reduce(MyKey keyIn, Iterable<IntWritable> values, Context context)
        throws IOException, InterruptedException {
      Iterator<IntWritable> iter = values.iterator();
      // sum values
      int sum = 0;
      while (iter.hasNext()) {
        sum += iter.next().get();
      }

      // keep original tuple key, emit sum of counts as value
      SumValue.set(sum);
      context.write(keyIn, SumValue);
    }
  }

  /**
   * Creates an instance of this tool.
   */
  public DemoWordCountJsonx() {
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
    int numReduceTasks = Integer.parseInt(args[2]);

    sLogger.info("Tool: DemoWordCountJSON");
    sLogger.info(" - input path: " + inputPath);
    sLogger.info(" - output path: " + outputPath);
    sLogger.info(" - number of reducers: " + numReduceTasks);

    Configuration conf = getConf();
    Job job = new Job(conf, "DemoWordCountJSON");
    job.setJarByClass(DemoWordCountJsonx.class);
    job.setNumReduceTasks(numReduceTasks);

    FileInputFormat.setInputPaths(job, new Path(inputPath));
    FileOutputFormat.setOutputPath(job, new Path(outputPath));

    job.setInputFormatClass(SequenceFileInputFormat.class);
    job.setOutputFormatClass(SequenceFileOutputFormat.class);

    job.setOutputKeyClass(MyKey.class);
    job.setOutputValueClass(IntWritable.class);

    job.setMapperClass(MyMapper.class);
    job.setCombinerClass(MyReducer.class);
    job.setReducerClass(MyReducer.class);

    // Delete the output directory if it exists already
    Path outputDir = new Path(outputPath);
    FileSystem.get(conf).delete(outputDir, true);

    long startTime = System.currentTimeMillis();
    job.waitForCompletion(true);
    sLogger.info("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0
        + " seconds");

    return 0;
  }

  /**
   * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
   */
  public static void main(String[] args) throws Exception {
    ToolRunner.run(new DemoWordCountJsonx(), args);
  }
}
