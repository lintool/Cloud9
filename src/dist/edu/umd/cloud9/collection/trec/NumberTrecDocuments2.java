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

package edu.umd.cloud9.collection.trec;

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
import org.apache.log4j.Logger;

/**
 * <p>
 * Program that builds the mapping from TREC docids (String identifiers) to docnos
 * (sequentially-numbered ints). The program takes four command-line arguments:
 * </p>
 *
 * <ul>
 * <li>[input] path to the document collection</li>
 * <li>[output-dir] path to temporary MapReduce output directory</li>
 * <li>[output-file] path to location of mappings file</li>
 * </ul>
 *
 * <p>
 * Here's a sample invocation:
 * </p>
 *
 * <blockquote><pre>
 * setenv HADOOP_CLASSPATH "/foo/cloud9-x.y.z.jar:/foo/guava-r09.jar"
 *
 * hadoop jar cloud9-x.y.z.jar edu.umd.cloud9.collection.trec.NumberTrecDocuments2 \
 *   -libjars=guava-r09.jar \
 *   /shared/collections/trec/trec4-5_noCRFR.xml \
 *   /user/jimmylin/trec-docid-tmp \
 *   /user/jimmylin/docno-mapping.dat
 * </pre></blockquote>
 * 
 * @author Jimmy Lin
 */
public class NumberTrecDocuments2 extends Configured implements Tool {
  private static final Logger LOG = Logger.getLogger(NumberTrecDocuments2.class);
  private static enum Count { DOCS };

  private static class MyMapper extends Mapper<LongWritable, TrecDocument, Text, IntWritable> {
    private static final Text docid = new Text();
    private static final IntWritable one = new IntWritable(1);

    @Override
    public void map(LongWritable key, TrecDocument doc, Context context)
        throws IOException, InterruptedException {
      context.getCounter(Count.DOCS).increment(1);
      docid.set(doc.getDocid());
      context.write(docid, one);
    }
  }

  private static class MyReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    private final static IntWritable cnt = new IntWritable(1);

    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context)
        throws IOException, InterruptedException {
      context.write(key, cnt);
      cnt.set(cnt.get() + 1);
    }
  }

  /**
   * Creates an instance of this tool.
   */
  public NumberTrecDocuments2() {
  }

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

    LOG.info("Tool: " + NumberTrecDocuments2.class.getCanonicalName());
    LOG.info(" - Input path: " + inputPath);
    LOG.info(" - Output path: " + outputPath);
    LOG.info(" - Output file: " + outputFile);

    Job job = new Job(getConf(), NumberTrecDocuments2.class.getSimpleName());
    job.setJarByClass(NumberTrecDocuments.class);

    job.setNumReduceTasks(1);

    FileInputFormat.setInputPaths(job, new Path(inputPath));
    FileOutputFormat.setOutputPath(job, new Path(outputPath));
    FileOutputFormat.setCompressOutput(job, false);

    job.setInputFormatClass(TrecDocumentInputFormat2.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    job.setOutputFormatClass(TextOutputFormat.class);

    job.setMapperClass(MyMapper.class);
    job.setReducerClass(MyReducer.class);

    // Delete the output directory if it exists already.
    FileSystem.get(job.getConfiguration()).delete(new Path(outputPath), true);

    job.waitForCompletion(true);
    
    String input = outputPath + (outputPath.endsWith("/") ? "" : "/") + "/part-r-00000";
    TrecDocnoMapping.writeMappingData(new Path(input), new Path(outputFile),
        FileSystem.get(getConf()));

    return 0;
  }

  /**
   * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
   */
  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new NumberTrecDocuments2(), args);
    System.exit(res);
  }
}
