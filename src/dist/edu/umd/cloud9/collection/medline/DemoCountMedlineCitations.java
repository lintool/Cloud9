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
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
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
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import edu.umd.cloud9.collection.DocnoMapping;

/**
 * <p>
 * Simple demo program that counts all the documents in a collection of MEDLINE citations. This
 * provides a skeleton for MapReduce programs to process the collection. The program takes three
 * command-line arguments:
 * </p>
 *
 * <ul>
 * <li>[input] path to the document collection
 * <li>[output-dir] path to the output directory
 * <li>[mappings-file] path to the docnos mappings file
 * </ul>
 *
 * <p>
 * Here's a sample invocation:
 * </p>
 *
 * <blockquote><pre>
 * setenv HADOOP_CLASSPATH "/foo/cloud9-x.y.z.jar:/foo/guava-r09.jar"
 *
 * hadoop jar cloud9-x.y.z.jar edu.umd.cloud9.collection.medline.DemoCountMedlineCitations \
 *   -libjars=guava-r09.jar \
 *   /shared/collections/medline04 \
 *   /user/jimmylin/count-tmp \
 *   /user/jimmylin/docno-mapping.dat
 * </pre></blockquote>
 *
 * @author Jimmy Lin
 */
@SuppressWarnings("deprecation")
public class DemoCountMedlineCitations extends Configured implements Tool {
  private static final Logger LOG = Logger.getLogger(DemoCountMedlineCitations.class);
  private static enum Count { DOCS };

  private static class MyMapper extends MapReduceBase implements
      Mapper<LongWritable, MedlineCitation, Text, IntWritable> {

    private final static Text outKey = new Text();
    private final static IntWritable outVal = new IntWritable(1);
    private DocnoMapping docMapping;

    public void configure(JobConf job) {
      try {
        Path[] localFiles = DistributedCache.getLocalCacheFiles(job);

        // Instead of hard-coding the actual concrete DocnoMapping class, have the name of the
        // class passed in as a property; this makes the mapper more general.
        docMapping = (DocnoMapping) Class.forName(job.get("DocnoMappingClass")).newInstance();

        // simply assume that the mappings file is the only file in the
        // distributed cache
        docMapping.loadMapping(localFiles[0], FileSystem.getLocal(job));
      } catch (Exception e) {
        e.printStackTrace();
        throw new RuntimeException("Error initializing DocnoMapping!");
      }
    }

    public void map(LongWritable key, MedlineCitation doc,
        OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
      reporter.incrCounter(Count.DOCS, 1);

      outKey.set(doc.getDocid());
      int docno = docMapping.getDocno(doc.getDocid());
      if ( docno <= 0) {
        throw new RuntimeException("Error, unable to find docno for docid " + doc.getDocid());
      }

      outVal.set(docno);
      output.collect(outKey, outVal);
    }
  }

  /**
   * Creates an instance of this tool.
   */
  public DemoCountMedlineCitations() {}

  private static int printUsage() {
    System.out.println("usage: [input] [output-dir] [mappings-file]");
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
    String mappingFile = args[2];

    LOG.info("input: " + inputPath);
    LOG.info("output dir: " + outputPath);
    LOG.info("docno mapping file: " + mappingFile);

    JobConf conf = new JobConf(getConf(), DemoCountMedlineCitations.class);
    conf.setJobName("DemoCountMedlineCitations");

    conf.setNumReduceTasks(0);

    // Pass in the class name as a String; this is makes the mapper general in being able to load
    // any collection of Indexable objects that has docid/docno mapping specified by a DocnoMapping
    // object.
    conf.set("DocnoMappingClass", MedlineDocnoMapping.class.getCanonicalName());

    // put the mapping file in the distributed cache so each map worker will
    // have it
    DistributedCache.addCacheFile(new URI(mappingFile), conf);

    FileInputFormat.setInputPaths(conf, new Path(inputPath));
    FileOutputFormat.setOutputPath(conf, new Path(outputPath));
    FileOutputFormat.setCompressOutput(conf, false);

    conf.setInputFormat(MedlineCitationInputFormat.class);
    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(IntWritable.class);

    conf.setMapperClass(MyMapper.class);

    // delete the output directory if it exists already
    FileSystem.get(conf).delete(new Path(outputPath), true);

    JobClient.runJob(conf);

    return 0;
  }

  /**
   * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
   */
  public static void main(String[] args) throws Exception {
    ToolRunner.run(new Configuration(), new DemoCountMedlineCitations(), args);
  }
}
