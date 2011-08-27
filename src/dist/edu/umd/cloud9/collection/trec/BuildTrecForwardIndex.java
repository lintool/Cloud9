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
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.lib.IdentityReducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import edu.umd.cloud9.collection.DocnoMapping;
import edu.umd.cloud9.io.FSLineReader;

/**
 * <p>
 * Tool for building a document forward index for TREC collections. Sameple Invocation:
 * </p>
 *
 * <pre>
 * hadoop jar cloud9.jar edu.umd.cloud9.collection.trec.BuildTrecForwardIndex \
 *  /umd-lin/shared/collections/trec/trec4-5_noCRFR.xml /tmp/findex/ \
 *  /umd-lin/shared/collections/trec4-5_noCRFR.findex.dat \
 *  /umd-lin/shared/indexes/trec/docno-mapping.dat
 * </pre>
 *
 * @author Jimmy Lin
 */
public class BuildTrecForwardIndex extends Configured implements Tool {
  private static final Logger LOG = Logger.getLogger(BuildTrecForwardIndex.class);
  private static enum Count { DOCS };

  private static final String DOCNO_MAPPING_FILE_PROPERTY = "DocnoMappingFile";

  private static class MyMapper extends MapReduceBase implements
      Mapper<LongWritable, TrecDocument, IntWritable, Text> {
    private static final IntWritable docno = new IntWritable(1);
    private static final Text text = new Text();
    private DocnoMapping docMapping;

    public void configure(JobConf job) {
      // load the docid to docno mappings
      try {
        docMapping = new TrecDocnoMapping();

        // Detect if we're in standalone mode; if so, we can't us the
        // DistributedCache because it does not (currently) work in
        // standalone mode...
        if (job.get("mapred.job.tracker").equals("local")) {
          FileSystem fs = FileSystem.get(job);
          String mappingFile = job.get(DOCNO_MAPPING_FILE_PROPERTY);
          docMapping.loadMapping(new Path(mappingFile), fs);
        } else {
          Path[] localFiles = DistributedCache.getLocalCacheFiles(job);
          docMapping.loadMapping(localFiles[0], FileSystem.getLocal(job));
        }
      } catch (Exception e) {
        e.printStackTrace();
        throw new RuntimeException("Error initializing DocnoMapping!");
      }
    }

    public void map(LongWritable key, TrecDocument doc, OutputCollector<IntWritable, Text> output,
        Reporter reporter) throws IOException {
      reporter.incrCounter(Count.DOCS, 1);

      int len = doc.getContent().getBytes().length;
      docno.set(docMapping.getDocno(doc.getDocid()));
      text.set(key + "\t" + len);
      output.collect(docno, text);
    }
  }

  public BuildTrecForwardIndex() {}

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

    JobConf conf = new JobConf(getConf(), DemoCountTrecDocuments.class);
    FileSystem fs = FileSystem.get(getConf());

    String collectionPath = args[0];
    String outputPath = args[1];
    String indexFile = args[2];
    String mappingFile = args[3];

    LOG.info("Tool name: BuildTrecForwardIndex");
    LOG.info(" - collection path: " + collectionPath);
    LOG.info(" - output path: " + outputPath);
    LOG.info(" - index file: " + indexFile);
    LOG.info(" - mapping file: " + mappingFile);

    conf.setJobName("BuildTrecForwardIndex");

    conf.set("mapred.child.java.opts", "-Xmx1024m");
    conf.setNumReduceTasks(1);

    if (conf.get("mapred.job.tracker").equals("local")) {
      conf.set(DOCNO_MAPPING_FILE_PROPERTY, mappingFile);
    } else {
      DistributedCache.addCacheFile(new URI(mappingFile), conf);
    }

    FileInputFormat.setInputPaths(conf, new Path(collectionPath));
    FileOutputFormat.setOutputPath(conf, new Path(outputPath));
    FileOutputFormat.setCompressOutput(conf, false);

    conf.setInputFormat(TrecDocumentInputFormat.class);
    conf.setOutputKeyClass(IntWritable.class);
    conf.setOutputValueClass(Text.class);

    conf.setMapperClass(MyMapper.class);
    conf.setReducerClass(IdentityReducer.class);

    // delete the output directory if it exists already
    FileSystem.get(conf).delete(new Path(outputPath), true);

    RunningJob job = JobClient.runJob(conf);

    Counters counters = job.getCounters();
    int numDocs = (int) counters.findCounter(Count.DOCS).getCounter();

    String inputFile = outputPath + "/" + "part-00000";

    LOG.info("Writing " + numDocs + " doc offseta to " + indexFile);
    FSLineReader reader = new FSLineReader(inputFile, fs);

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
    int res = ToolRunner.run(conf, new BuildTrecForwardIndex(), args);
    System.exit(res);
  }
}
