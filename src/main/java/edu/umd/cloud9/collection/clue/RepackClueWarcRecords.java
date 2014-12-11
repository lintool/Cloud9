/*
 * Cloud9: A Hadoop toolkit for working with big data
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

package edu.umd.cloud9.collection.clue;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

/**
 * <p>
 * Program to uncompress the ClueWeb09 collection from the original distribution WARC files and
 * repack as <code>SequenceFiles</code>.
 * </p>
 *
 * <p>
 * The program takes the following command-line arguments:
 * </p>
 *
 * <ul>
 * <li>[base-path] base path of the ClueWeb09 distribution</li>
 * <li>[output-path] output path</li>
 * <li>[segment-num] segment number (1 through 10)</li>
 * <li>[docno-mapping-data-file] docno mapping data file</li>
 * <li>(block|record|none) to indicate block-compression, record-compression, or no compression</li>
 * </ul>
 *
 * <p>
 * Here's a sample invocation:
 * </p>
 *
 * <pre>
 * hadoop jar dist/cloud9-X.X.X.jar edu.umd.cloud9.collection.clue.RepackClueWarcRecords \
 *  /shared/collections/ClueWeb09/collection.raw \
 *  /shared/collections/ClueWeb09/collection.compressed.block/en.01 1 \
 *  /shared/collections/ClueWeb09/docno-mapping.dat block
 * </pre>
 *
 * @author Jimmy Lin
 */
public class RepackClueWarcRecords extends Configured implements Tool {
  private static final Logger LOG = Logger.getLogger(RepackClueWarcRecords.class);

  private static enum Records { TOTAL, PAGES };

  private static class MyMapper extends MapReduceBase implements
      Mapper<LongWritable, ClueWarcRecord, IntWritable, ClueWarcRecord> {

    private static final IntWritable DOCNO = new IntWritable();
    private ClueWarcDocnoMapping docnoMapping = new ClueWarcDocnoMapping();

    public void configure(JobConf job) {
      try {
        docnoMapping.loadMapping(new Path(job.get("DocnoMappingDataFile")), FileSystem.get(job));
      } catch (Exception e) {
        throw new RuntimeException("Error loading docno mapping data file!");
      }
    }

    public void map(LongWritable key, ClueWarcRecord doc,
        OutputCollector<IntWritable, ClueWarcRecord> output, Reporter reporter) throws IOException {
      reporter.incrCounter(Records.TOTAL, 1);

      String id = doc.getHeaderMetadataItem("WARC-TREC-ID");

      if (id != null) {
        reporter.incrCounter(Records.PAGES, 1);

        DOCNO.set(docnoMapping.getDocno(id));
        output.collect(DOCNO, doc);
      }
    }
  }

  /**
   * Creates an instance of this tool.
   */
  public RepackClueWarcRecords() {
  }

  private static int printUsage() {
    System.out.println("usage: [base-path] [output-path] [segment-num] [docno-mapping-data-file] (block|record|none)");
    ToolRunner.printGenericCommandUsage(System.out);
    return -1;
  }

  /**
   * Runs this tool.
   */
  public int run(String[] args) throws Exception {
    if (args.length != 5) {
      printUsage();
      return -1;
    }

    String basePath = args[0];
    String outputPath = args[1];
    int segment = Integer.parseInt(args[2]);
    String data = args[3];
    String compressionType = args[4];

    if (!compressionType.equals("block") && !compressionType.equals("record")
        && !compressionType.equals("none")) {
      System.err.println("Error: \"" + compressionType + "\" unknown compression type!");
      System.exit(-1);
    }

    // Default block size.
    int blocksize = 1000000;

    JobConf conf = new JobConf(RepackClueWarcRecords.class);
    conf.setJobName("RepackClueWarcRecords:segment" + segment);

    conf.set("DocnoMappingDataFile", data);

    LOG.info("Tool name: RepackClueWarcRecords");
    LOG.info(" - base path: " + basePath);
    LOG.info(" - output path: " + outputPath);
    LOG.info(" - segment number: " + segment);
    LOG.info(" - docno mapping data file: " + data);
    LOG.info(" - compression type: " + compressionType);

    if (compressionType.equals("block")) {
      LOG.info(" - block size: " + blocksize);
    }

    int mapTasks = 10;

    conf.setNumMapTasks(mapTasks);
    conf.setNumReduceTasks(0);

    ClueCollectionPathConstants.addEnglishCollectionPart(conf, basePath, segment);

    SequenceFileOutputFormat.setOutputPath(conf, new Path(outputPath));

    if (compressionType.equals("none")) {
      SequenceFileOutputFormat.setCompressOutput(conf, false);
    } else {
      SequenceFileOutputFormat.setCompressOutput(conf, true);

      if (compressionType.equals("record")) {
        SequenceFileOutputFormat
            .setOutputCompressionType(conf, SequenceFile.CompressionType.RECORD);
      } else {
        SequenceFileOutputFormat.setOutputCompressionType(conf, SequenceFile.CompressionType.BLOCK);
        conf.setInt("io.seqfile.compress.blocksize", blocksize);
      }
    }

    conf.setInputFormat(ClueWarcInputFormat.class);
    conf.setOutputFormat(SequenceFileOutputFormat.class);
    conf.setOutputKeyClass(IntWritable.class);
    conf.setOutputValueClass(ClueWarcRecord.class);

    conf.setMapperClass(MyMapper.class);

    // Delete the output directory if it exists already.
    FileSystem.get(conf).delete(new Path(outputPath), true);

    JobClient.runJob(conf);

    return 0;
  }

  /**
   * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
   */
  public static void main(String[] args) throws Exception {
    LOG.info("Running " + RepackClueWarcRecords.class.getCanonicalName() + " with args "
        + Arrays.toString(args));
    ToolRunner.run(new RepackClueWarcRecords(), args);
  }
}
