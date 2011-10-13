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
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.LineReader;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import edu.umd.cloud9.collection.DocnoMapping;

/**
 * <p>
 * Tool for building a document forward index for TREC collections. Sample Invocation:
 * </p>
 *
 * <blockquote><pre>
 * setenv HADOOP_CLASSPATH "/foo/cloud9-x.y.z.jar:/foo/guava-r09.jar"
 *
 * hadoop jar cloud9-x.y.z.jar edu.umd.cloud9.collection.trec.BuildTrecForwardIndex2 \
 *   -libjars=guava-r09.jar \
 *   /shared/collections/trec/trec4-5_noCRFR.xml \
 *   /user/jimmylin/tmp \
 *   /user/jimmylin/findex.dat \
 *   /user/jimmylin/docno-mapping.dat
 * </pre></blockquote>
 *
 * @author Jimmy Lin
 */
public class BuildTrecForwardIndex2 extends Configured implements Tool {
  private static final Logger LOG = Logger.getLogger(BuildTrecForwardIndex2.class);
  private static enum Count { DOCS };

  private static final String DOCNO_MAPPING_FILE_PROPERTY = "DocnoMappingFile";

  private static class MyMapper extends Mapper<LongWritable, TrecDocument, IntWritable, Text> {
    private static final IntWritable docno = new IntWritable(1);
    private static final Text text = new Text();
    private DocnoMapping docMapping;

    @Override
    public void setup(Context context) {
      // load the docid to docno mappings
      try {
        Configuration conf = context.getConfiguration();
        docMapping = new TrecDocnoMapping();

        // Detect if we're in standalone mode; if so, we can't us the
        // DistributedCache because it does not (currently) work in
        // standalone mode...
        if (conf.get("mapred.job.tracker").equals("local")) {
          FileSystem fs = FileSystem.get(conf);
          String mappingFile = conf.get(DOCNO_MAPPING_FILE_PROPERTY);
          docMapping.loadMapping(new Path(mappingFile), fs);
        } else {
          Path[] localFiles = DistributedCache.getLocalCacheFiles(conf);
          docMapping.loadMapping(localFiles[0], FileSystem.getLocal(conf));
        }
      } catch (Exception e) {
        e.printStackTrace();
        throw new RuntimeException("Error initializing DocnoMapping!");
      }
    }

    @Override
    public void map(LongWritable key, TrecDocument doc, Context context)
        throws IOException, InterruptedException {
      context.getCounter(Count.DOCS).increment(1);

      int len = doc.getContent().getBytes().length;
      docno.set(docMapping.getDocno(doc.getDocid()));
      text.set(key + "\t" + len);
      context.write(docno, text);
    }
  }

  public BuildTrecForwardIndex2() {
  }

  private static int printUsage() {
    System.out.println("usage: [collection-path] [output-path] [index-file] [docno-mapping-file]");
    ToolRunner.printGenericCommandUsage(System.out);
    return -1;
  }

  /**
   * Runs this tool.
   */
  public int run(String[] args) throws Exception {
    if (args.length != 4) {
      printUsage();
      return -1;
    }

    Job job = new Job(getConf(), BuildTrecForwardIndex2.class.getCanonicalName());
    job.setJarByClass(BuildTrecForwardIndex2.class);
    FileSystem fs = FileSystem.get(getConf());

    String collectionPath = args[0];
    String outputPath = args[1];
    String indexFile = args[2];
    String mappingFile = args[3];

    LOG.info("Tool name: " + BuildTrecForwardIndex2.class.getSimpleName());
    LOG.info(" - collection path: " + collectionPath);
    LOG.info(" - output path: " + outputPath);
    LOG.info(" - index file: " + indexFile);
    LOG.info(" - mapping file: " + mappingFile);

    job.getConfiguration().set("mapred.child.java.opts", "-Xmx1024m");
    job.setNumReduceTasks(1);

    if (job.getConfiguration().get("mapred.job.tracker").equals("local")) {
      job.getConfiguration().set(DOCNO_MAPPING_FILE_PROPERTY, mappingFile);
    } else {
      DistributedCache.addCacheFile(new URI(mappingFile), job.getConfiguration());
    }

    FileInputFormat.setInputPaths(job, new Path(collectionPath));
    FileOutputFormat.setOutputPath(job, new Path(outputPath));
    FileOutputFormat.setCompressOutput(job, false);

    job.setInputFormatClass(TrecDocumentInputFormat2.class);
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(Text.class);

    job.setMapperClass(MyMapper.class);

    // delete the output directory if it exists already
    FileSystem.get(getConf()).delete(new Path(outputPath), true);

    job.waitForCompletion(true);
    Counters counters = job.getCounters();
    int numDocs = (int) counters.findCounter(Count.DOCS).getValue();

    String inputFile = outputPath + "/" + "part-r-00000";

    LOG.info("Writing " + numDocs + " doc offseta to " + indexFile);
    LineReader reader = new LineReader(fs.open(new Path(inputFile)));

    FSDataOutputStream writer = fs.create(new Path(indexFile), true);

    writer.writeUTF(edu.umd.cloud9.collection.trec.TrecForwardIndex.class.getCanonicalName());
    writer.writeUTF(collectionPath);
    writer.writeInt(numDocs);

    int cnt = 0;
    Text line = new Text();
    while (reader.readLine(line) > 0) {
      String[] arr = line.toString().split("\\t");
      long offset = Long.parseLong(arr[1]);
      int len = Integer.parseInt(arr[2]);

      writer.writeLong(offset);
      writer.writeInt(len);

      cnt++;
      if (cnt % 100000 == 0) {
        LOG.info(cnt + " docs");
      }
    }
    reader.close();
    writer.close();
    LOG.info(cnt + " docs total. Done!");

    if (numDocs != cnt) {
      throw new RuntimeException("Unexpected number of documents in building forward index!");
    }

    return 0;
  }

  /**
   * Dispatches command-line arguments to the tool via the <code>ToolRunner</code>.
   */
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    int res = ToolRunner.run(conf, new BuildTrecForwardIndex2(), args);
    System.exit(res);
  }
}
