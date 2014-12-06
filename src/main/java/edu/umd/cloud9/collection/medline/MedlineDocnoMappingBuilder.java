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

package edu.umd.cloud9.collection.medline;

import java.io.IOException;
import java.util.Random;

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
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import edu.umd.cloud9.collection.DocnoMapping;

/**
 * Tool that builds the mapping from MEDLINE docids (PMIDs) to docnos (sequentially-numbered ints).
 * Run without any arguments for help. The guava jar must be included using {@code -libjar}.
 *
 * @author Jimmy Lin
 */
public class MedlineDocnoMappingBuilder extends Configured implements Tool, DocnoMapping.Builder {
  private static final Logger LOG = Logger.getLogger(MedlineDocnoMappingBuilder.class);
  private static final Random random = new Random();
  private static enum Count { DOCS };

  private static class MyMapper extends Mapper<LongWritable, MedlineCitation, IntWritable, IntWritable> {
    private static final IntWritable docid = new IntWritable();
    private static final IntWritable one = new IntWritable(1);

    @Override
    public void map(LongWritable key, MedlineCitation doc, Context context)
        throws IOException, InterruptedException {
      context.getCounter(Count.DOCS).increment(1);
      docid.set(Integer.parseInt(doc.getDocid()));
      context.write(docid, one);
    }
  }

  private static class MyReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
    private final static IntWritable cnt = new IntWritable(1);

    @Override
    public void reduce(IntWritable key, Iterable<IntWritable> values, Context context)
        throws IOException, InterruptedException {
      context.write(key, cnt);
      cnt.set(cnt.get() + 1);
    }
  }

  /**
   * Creates an instance of this tool.
   */
  public MedlineDocnoMappingBuilder() {}

  @Override
  public int build(Path src, Path dest, Configuration conf) throws IOException {
    super.setConf(conf);
    return run(new String[] {
        "-" + DocnoMapping.BuilderUtils.COLLECTION_OPTION + "=" + src.toString(),
        "-" + DocnoMapping.BuilderUtils.MAPPING_OPTION + "=" + dest.toString() });
  }

  /**
   * Runs this tool.
   */
  public int run(String[] args) throws IOException {
    DocnoMapping.DefaultBuilderOptions options = DocnoMapping.BuilderUtils.parseDefaultOptions(args);
    if ( options == null) {
      return -1;
    }

    // Temp directory.
    String tmpDir = "tmp-" + MedlineDocnoMappingBuilder.class.getSimpleName() + "-" + random.nextInt(10000);

    LOG.info("Tool: " + MedlineDocnoMappingBuilder.class.getCanonicalName());
    LOG.info(" - input path: " + options.collection);
    LOG.info(" - output file: " + options.docnoMapping);

    Job job = new Job(getConf(),
        MedlineDocnoMappingBuilder.class.getSimpleName() + ":" + options.collection);
    FileSystem fs = FileSystem.get(job.getConfiguration());

    job.setJarByClass(MedlineDocnoMappingBuilder.class);

    job.setNumReduceTasks(1);

    FileInputFormat.setInputPaths(job, new Path(options.collection));
    FileOutputFormat.setOutputPath(job, new Path(tmpDir));
    FileOutputFormat.setCompressOutput(job, false);

    job.setInputFormatClass(MedlineCitationInputFormat.class);
    job.setOutputKeyClass(IntWritable.class);
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

    String input = tmpDir + (tmpDir.endsWith("/") ? "" : "/") + "/part-r-00000";
    MedlineDocnoMapping.writeMappingData(new Path(input), new Path(options.docnoMapping),
        FileSystem.get(getConf()));

    fs.delete(new Path(tmpDir), true);

    return 0;
  }

  /**
   * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
   */
  public static void main(String[] args) throws Exception {
    ToolRunner.run(new Configuration(), new MedlineDocnoMappingBuilder(), args);
  }
}
