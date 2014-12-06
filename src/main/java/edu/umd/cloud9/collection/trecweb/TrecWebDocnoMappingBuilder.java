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
import java.util.Arrays;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.LineReader;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import edu.umd.cloud9.collection.DocnoMapping;

/**
 * Tool that builds the mapping from docids (String identifiers) to docnos (sequentially-numbered
 * ints) for TREC web collections (wt10g, gov2). Run without any arguments for help. The guava jar
 * must be included using {@code -libjar}.
 *
 * @author Jimmy Lin
 */
public class TrecWebDocnoMappingBuilder extends Configured implements Tool, DocnoMapping.Builder {
  private static final Logger LOG = Logger.getLogger(TrecWebDocnoMappingBuilder.class);
  private static final Random random = new Random();

  protected static enum Documents { Total };

  private static class MyMapper extends Mapper<LongWritable, TrecWebDocument, Text, IntWritable> {
    private final static Text text = new Text();
    private final static IntWritable out = new IntWritable(1);

    @Override
    public void map(LongWritable key, TrecWebDocument doc, Context context)
        throws IOException, InterruptedException {
      context.getCounter(Documents.Total).increment(1);
      text.set(doc.getDocid());
      context.write(text, out);
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

  public TrecWebDocnoMappingBuilder() {}

  @Override
  public int build(Path src, Path dest, Configuration conf) throws IOException {
    super.setConf(conf);
    return run(new String[] {
        "-" + DocnoMapping.BuilderUtils.COLLECTION_OPTION + "=" + src.toString(),
        "-" + DocnoMapping.BuilderUtils.MAPPING_OPTION + "=" + dest.toString() });
  }

  @Override
  public int run(String[] args) throws IOException {
    DocnoMapping.DefaultBuilderOptions options = DocnoMapping.BuilderUtils.parseDefaultOptions(args);
    if ( options == null) {
      return -1;
    }

    // Temp directory.
    String tmpDir = "tmp-" + TrecWebDocnoMappingBuilder.class.getSimpleName() +
        "-" + random.nextInt(10000);

    LOG.info("Tool name: " + TrecWebDocnoMappingBuilder.class.getCanonicalName());
    LOG.info(" - input path: " + options.collection);
    LOG.info(" - output file: " + options.docnoMapping);

    Job job = new Job(getConf(),
        TrecWebDocnoMappingBuilder.class.getSimpleName() + ":" + options.collection);
    FileSystem fs = FileSystem.get(job.getConfiguration());

    job.setJarByClass(TrecWebDocnoMappingBuilder.class);

    job.setNumReduceTasks(1);

    PathFilter filter = new PathFilter() {
      @Override public boolean accept(Path path) {
        return !path.getName().startsWith("_");
      }
    };

    // Note: Gov2 and Wt10g raw collections are organized into sub-directories.
    Path collectionPath = new Path(options.collection);
    for (FileStatus status : fs.listStatus(collectionPath, filter)) {
      if ( status.isDirectory()) {
        for (FileStatus s : fs.listStatus(status.getPath(), filter)) {
          FileInputFormat.addInputPath(job, s.getPath());
        }
      } else {
        FileInputFormat.addInputPath(job, status.getPath());
      }
    }
    FileOutputFormat.setOutputPath(job, new Path(tmpDir));
    FileOutputFormat.setCompressOutput(job, false);

    job.setInputFormatClass(options.inputFormat);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    job.setOutputFormatClass(TextOutputFormat.class);

    job.setMapperClass(MyMapper.class);
    job.setReducerClass(MyReducer.class);

    // Delete the output directory if it exists already.
    fs.delete(new Path(tmpDir), true);

    try {
      job.waitForCompletion(true);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    writeMappingData(new Path(tmpDir + "/part-r-00000"), new Path(options.docnoMapping), fs);
    fs.delete(new Path(tmpDir), true);

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
    LOG.info("Running " + TrecWebDocnoMappingBuilder.class.getCanonicalName() +
        " with args " + Arrays.toString(args));
    ToolRunner.run(new Configuration(), new TrecWebDocnoMappingBuilder(), args);
  }
}
