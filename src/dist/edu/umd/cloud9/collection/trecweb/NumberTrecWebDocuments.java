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

package edu.umd.cloud9.collection.trecweb;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataOutputStream;
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
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.LineReader;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

/**
 * Numbers documents in TREC web collections that have been repacked into {@code SequenceFile}s.
 * Sample invocations:
 *
 * <blockquote><pre>
 * setenv HADOOP_CLASSPATH "/foo/cloud9-x.y.z.jar:/foo/guava-r09.jar"
 *
 * hadoop jar cloud9-x.y.z.jar edu.umd.cloud9.collection.trecweb.NumberTrecWebDocuments \
 *   -libjars=guava-r09.jar \
 *   /shared/collections/wt10g/collection.compressed.block \
 *   /user/jimmylin/tmp \
 *   /user/jimmylin/docno-mapping.dat 100
 *
 * hadoop jar cloud9-x.y.z.jar edu.umd.cloud9.collection.trecweb.NumberTrecWebDocuments \
 *   -libjars=guava-r09.jar \
 *   /shared/collections/gov2/collection.compressed.block \
 *   /user/jimmylin/tmp \
 *   /user/jimmylin/docno-mapping.dat 100
 * </pre></blockquote>
 *
 * @author Jimmy Lin
 */
@SuppressWarnings("deprecation")
public class NumberTrecWebDocuments extends Configured implements Tool {
  private static final Logger LOG = Logger.getLogger(NumberTrecWebDocuments.class);
  protected static enum Documents { Total };

  private static class MyMapper extends MapReduceBase implements
      Mapper<LongWritable, TrecWebDocument, Text, IntWritable> {
    private final static Text text = new Text();
    private final static IntWritable out = new IntWritable(1);

    public void map(LongWritable key, TrecWebDocument doc,
        OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
      reporter.incrCounter(Documents.Total, 1);

      System.out.println(doc.getDocid());
      text.set(doc.getDocid());
      output.collect(text, out);
    }
  }

  private static class MyReducer extends MapReduceBase implements
      Reducer<Text, IntWritable, Text, IntWritable> {
    private final static IntWritable cnt = new IntWritable(1);

    public void reduce(Text key, Iterator<IntWritable> values,
        OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
      output.collect(key, cnt);
      cnt.set(cnt.get() + 1);
    }
  }

  public NumberTrecWebDocuments() {}

  public int run(String[] args) throws Exception {
    if (args.length != 4) {
      System.out.println("usage: [input] [output-dir] [output-file] [num-mappers]");
      System.exit(-1);
    }

    String inputPath = args[0];
    String outputPath = args[1];
    String outputFile = args[2];
    int mapTasks = Integer.parseInt(args[3]);

    LOG.info("Tool name: " + NumberTrecWebDocuments.class.getCanonicalName());
    LOG.info(" - input path: " + inputPath);
    LOG.info(" - output path: " + outputPath);
    LOG.info(" - output file: " + outputFile);
    LOG.info(" - number of mappers: " + mapTasks);

    JobConf conf = new JobConf(getConf(), NumberTrecWebDocuments.class);
    conf.setJobName(NumberTrecWebDocuments.class.getSimpleName());

    conf.setNumMapTasks(mapTasks);
    conf.setNumReduceTasks(1);

    FileInputFormat.setInputPaths(conf, new Path(inputPath));
    FileOutputFormat.setOutputPath(conf, new Path(outputPath));
    FileOutputFormat.setCompressOutput(conf, false);

    conf.setInputFormat(SequenceFileInputFormat.class);
    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(IntWritable.class);
    conf.setOutputFormat(TextOutputFormat.class);

    conf.setMapperClass(MyMapper.class);
    conf.setReducerClass(MyReducer.class);

    // Delete the output directory if it exists already.
    FileSystem.get(conf).delete(new Path(outputPath), true);

    JobClient.runJob(conf);

    writeMappingData(new Path(outputPath + "/part-00000"), new Path(outputFile),
        FileSystem.get(conf));

    return 0;
  }

  private static void writeMappingData(Path input, Path output, FileSystem fs) throws IOException {
    LOG.info("Writing docids to " + output);
    LineReader reader = new LineReader(fs.open(input));

    LOG.info("Reading " + input);
    int cnt = 0;
    Text line = new Text();
    while (reader.readLine(line) > 0) {
      cnt++;
    }
    reader.close();
    LOG.info("Done!");

    LOG.info("Writing " + output);
    FSDataOutputStream out = fs.create(output, true);
    reader = new LineReader(fs.open(input));
    out.writeInt(cnt);
    cnt = 0;
    while (reader.readLine(line) > 0) {
      String[] arr = line.toString().split("\\t");
      out.writeUTF(arr[0]);
      cnt++;
      if (cnt % 100000 == 0) {
        LOG.info(cnt + " documents");
      }
    }
    reader.close();
    out.close();
    LOG.info("Done! " + cnt + " documents total.");
  }

  /**
   * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
   */
  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new NumberTrecWebDocuments(), args);
    System.exit(res);
  }
}
