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

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

/**
 * <p>
 * Program to uncompress the gov2 collection from the original distribution and
 * repack as <code>SequenceFiles</code>.
 * </p>
 *
 * <p>
 * The program takes three command-line arguments:
 * </p>
 *
 * <ul>
 * <li>[base-path] base path of the ClueWeb09 distribution</li>
 * <li>[output-path] output path</li>
 * <li>(block|record|none) to indicate block-compression, record-compression,
 * or no compression</li>
 * </ul>
 *
 * @author Jimmy Lin
 */
public class RepackTrecWebCollection extends Configured implements Tool {
  private static final Logger LOG = Logger.getLogger(RepackTrecWebCollection.class);

  private static enum Documents { Count };

  private static class MyMapper extends
      Mapper<LongWritable, TrecWebDocument, LongWritable, TrecWebDocument> {
    @Override
    public void map(LongWritable key, TrecWebDocument doc, Context context)
        throws IOException, InterruptedException {
      context.getCounter(Documents.Count).increment(1);
      context.write(key, doc);
    }
  }

  private RepackTrecWebCollection() {}

  public static final String COLLECTION_OPTION = "collection";
  public static final String OUTPUT_OPTION = "output";
  public static final String COMPRESSION_OPTION = "compressionType";

  /**
   * Runs this tool.
   */
  @SuppressWarnings("static-access")
  public int run(String[] args) throws Exception {
    Options options = new Options();
    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("(required) collection path").create(COLLECTION_OPTION));
    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("(required) output path").create(OUTPUT_OPTION));
    options.addOption(OptionBuilder.withArgName("type").hasArg()
        .withDescription("(required) compression type: 'block', 'record', or 'none'")
        .create(COMPRESSION_OPTION));

    CommandLine cmdline;
    CommandLineParser parser = new GnuParser();
    try {
      cmdline = parser.parse(options, args);
    } catch (ParseException exp) {
      System.err.println("Error parsing command line: " + exp.getMessage());
      return -1;
    }

    if (!cmdline.hasOption(COLLECTION_OPTION) || !cmdline.hasOption(OUTPUT_OPTION) ||
        !cmdline.hasOption(COMPRESSION_OPTION)) {
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp(this.getClass().getName(), options);
      ToolRunner.printGenericCommandUsage(System.out);
      return -1;
    }

    String collection = cmdline.getOptionValue(COLLECTION_OPTION);
    String output = cmdline.getOptionValue(OUTPUT_OPTION);
    String compressionType = cmdline.getOptionValue(COMPRESSION_OPTION);

    if (!compressionType.equals("block") && !compressionType.equals("record")
        && !compressionType.equals("none")) {
      System.err.println("Error: \"" + compressionType + "\" unknown compression type!");
      System.exit(-1);
    }

    // This is the default block size.
    int blocksize = 1000000;

    Job job = new Job(getConf(), RepackTrecWebCollection.class.getSimpleName());
    FileSystem fs = FileSystem.get(job.getConfiguration());

    job.setJarByClass(RepackTrecWebCollection.class);

    LOG.info("Tool name: " + RepackTrecWebCollection.class.getCanonicalName());
    LOG.info(" - collection path: " + collection);
    LOG.info(" - output path: " + output);
    LOG.info(" - compression type: " + compressionType);

    if (compressionType.equals("block")) {
      LOG.info(" - block size: " + blocksize);
    }

    job.setNumReduceTasks(100);

    Path collectionPath = new Path(collection);
    for (FileStatus status : fs.listStatus(collectionPath)) {
      if ( status.isDir()) {
        for (FileStatus s : fs.listStatus(status.getPath())) {
          FileInputFormat.addInputPath(job, s.getPath());
        }
      } else {
        FileInputFormat.addInputPath(job, status.getPath());
      }
    }

    FileOutputFormat.setOutputPath(job, new Path(output));

    if (compressionType.equals("none")) {
      SequenceFileOutputFormat.setCompressOutput(job, false);
    } else {
      SequenceFileOutputFormat.setCompressOutput(job, true);

      if (compressionType.equals("record")) {
        SequenceFileOutputFormat.setOutputCompressionType(job, SequenceFile.CompressionType.RECORD);
      } else {
        SequenceFileOutputFormat.setOutputCompressionType(job, SequenceFile.CompressionType.BLOCK);
        job.getConfiguration().setInt("io.seqfile.compress.blocksize", blocksize);
      }
    }

    job.setInputFormatClass(TrecWebDocumentInputFormat.class);
    job.setOutputFormatClass(SequenceFileOutputFormat.class);
    job.setOutputKeyClass(LongWritable.class);
    job.setOutputValueClass(TrecWebDocument.class);

    job.setMapperClass(MyMapper.class);
    
    // delete the output directory if it exists already
    fs.delete(new Path(output), true);

    try {
      job.waitForCompletion(true);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    return 0;
  }

  /**
   * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
   */
  public static void main(String[] args) throws Exception {
    LOG.info("Running " + RepackTrecWebCollection.class.getCanonicalName() +
        " with args " + Arrays.toString(args));
    int res = ToolRunner.run(new Configuration(), new RepackTrecWebCollection(), args);
    System.exit(res);
  }
}