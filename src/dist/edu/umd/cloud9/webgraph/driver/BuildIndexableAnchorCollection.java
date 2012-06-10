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
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.lib.IdentityReducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import edu.umd.cloud9.collection.DocnoMapping;
import edu.umd.cloud9.io.array.ArrayListWritable;
import edu.umd.cloud9.webgraph.DriverUtil;
import edu.umd.cloud9.webgraph.data.AnchorText;
import edu.umd.cloud9.webgraph.data.IndexableAnchorText;

/**
 * Creates an indexable collection of anchors.
 *
 * @author Nima Asadi
 *
 */
public class BuildIndexableAnchorCollection extends Configured implements Tool {
  private static final Logger LOG = Logger.getLogger(BuildIndexableAnchorCollection.class);

  public static class MyMapper extends MapReduceBase implements
      Mapper<IntWritable, ArrayListWritable<AnchorText>, IntWritable, IndexableAnchorText> {
    private static final IndexableAnchorText sOutputValue = new IndexableAnchorText();
    private static DocnoMapping docnoMapping;
    private static int maxContentLength;

    public void configure(JobConf job) {
      maxContentLength = job.getInt("Cloud9.maxContentLength", 0);
      String docnoMappingClass = job.get("Cloud9.DocnoMappingClass", "edu.umd.cloud9.collection.clue.ClueWarcDocnoMapping");
      try {
        docnoMapping = (DocnoMapping) Class.forName(docnoMappingClass).newInstance();
      } catch(Exception e) {
        throw new RuntimeException("Class " + docnoMappingClass + " not found!");
      }

      Path[] localFiles;
      try {
        localFiles = DistributedCache.getLocalCacheFiles(job);
      } catch (IOException e) {
        throw new RuntimeException("Local cache files not read properly.");
      }

      try {
        docnoMapping.loadMapping(localFiles[0], FileSystem.getLocal(job));
      } catch (Exception e) {
        e.printStackTrace();
        throw new RuntimeException("Error initializing DocnoMapping!");
      }
    }

    public void map(IntWritable key, ArrayListWritable<AnchorText> value,
        OutputCollector<IntWritable, IndexableAnchorText> output, Reporter reporter) throws IOException {
      sOutputValue.clear();
      sOutputValue.setDocid(docnoMapping.getDocid(key.get()));

      if(maxContentLength > 0) {
        sOutputValue.concatenateAnchors(value, maxContentLength);
      } else {
        sOutputValue.concatenateAnchors(value);
      }

      output.collect(key, sOutputValue);
    }
  }

  public BuildIndexableAnchorCollection() {
  }

  private static int printUsage() {
    System.out.println("usage: [-input collection-path] [-output output-path]" +
                       " [-docnoClass docno-mapping-class] [-docno docno-mapping-file]" +
                       " [-numReducers num-reducers] [optional:-maxLength maximum content length]");
    ToolRunner.printGenericCommandUsage(System.out);
    return -1;
  }

  /**
   * Runs this tool.
   */
  public int run(String[] args) throws Exception {
    if (args.length < 5) {
      printUsage();
      return -1;
    }

    JobConf conf = new JobConf(getConf());
    FileSystem fs = FileSystem.get(conf);

    String collectionPath = DriverUtil.argValue(args, DriverUtil.CL_INPUT);
    String outputPath = DriverUtil.argValue(args, DriverUtil.CL_OUTPUT);
    String docnoMappingClass = DriverUtil.argValue(args, DriverUtil.CL_DOCNO_MAPPING_CLASS);
    String docnoMapping = DriverUtil.argValue(args, DriverUtil.CL_DOCNO_MAPPING);
    int numReducers = Integer.parseInt(DriverUtil.argValue(args, DriverUtil.CL_NUMBER_OF_REDUCERS));
    if(DriverUtil.argExists(args, DriverUtil.CL_MAX_LENGTH)) {
      conf.setInt("Cloud9.maxContentLength", Integer.parseInt(DriverUtil.argValue(args, DriverUtil.CL_MAX_LENGTH)));
    }
    conf.set("Cloud9.DocnoMappingClass", docnoMappingClass);

    LOG.info("Tool name: BuildAnchorTextForwardIndex");
    LOG.info(" - collection path: " + collectionPath);
    LOG.info(" - output path: " + outputPath);
    LOG.info(" - docno-mapping class: " + docnoMappingClass);
    LOG.info(" - docno-mapping file: " + docnoMapping);
    if(args.length == 6) {
      LOG.info(" - maximum content length: " + conf.getInt("Cloud9.maxContentLength", 0));
    }

    conf.set("mapred.child.java.opts", "-Xmx2048m");
    conf.setJobName("BuildIndexableAnchorCollection");
    conf.setJarByClass(BuildIndexableAnchorCollection.class);

    conf.setNumMapTasks(100);
    conf.setNumReduceTasks(numReducers);
    DistributedCache.addCacheFile(new URI(docnoMapping), conf);

    conf.setInputFormat(SequenceFileInputFormat.class);
    conf.setOutputFormat(SequenceFileOutputFormat.class);
    SequenceFileOutputFormat.setCompressOutput(conf, true);
    SequenceFileOutputFormat.setOutputCompressionType(conf, SequenceFile.CompressionType.BLOCK);
    SequenceFileInputFormat.setInputPaths(conf, new Path(collectionPath));
    SequenceFileOutputFormat.setOutputPath(conf, new Path(outputPath));

    conf.setOutputKeyClass(IntWritable.class);
    conf.setOutputValueClass(IndexableAnchorText.class);

    conf.setMapperClass(MyMapper.class);
    conf.setReducerClass(IdentityReducer.class);

    // delete the output directory if it exists already
    fs.delete(new Path(outputPath), true);
    RunningJob job = JobClient.runJob(conf);

    return 0;
  }

  /**
   * Dispatches command-line arguments to the tool via the
   * <code>ToolRunner</code>.
   */
  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new BuildIndexableAnchorCollection(), args);
    System.exit(res);
  }
}
