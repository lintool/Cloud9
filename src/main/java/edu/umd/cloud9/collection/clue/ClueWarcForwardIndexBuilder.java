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
import java.util.Random;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapRunnable;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.lib.IdentityReducer;
import org.apache.hadoop.util.LineReader;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import edu.umd.cloud9.mapred.NoSplitSequenceFileInputFormat;

/**
 * <p>
 * Tool for building a document forward index for the ClueWeb09 collection. Sample invocation:
 * </p>
 *
 * <pre>
 * hadoop jar dist/cloud9-X.X.X.jar edu.umd.cloud9.collection.clue.ClueWarcForwardIndexBuilder \
 *  -collection /shared/collections/ClueWeb09/collection.compressed.block/en.01 \
 *  -index findex.en.01.dat
 * </pre>
 *
 * @author Jimmy Lin
 */
public class ClueWarcForwardIndexBuilder extends Configured implements Tool {
  private static final Logger LOG = Logger.getLogger(ClueWarcForwardIndexBuilder.class);

  private static enum Blocks { Total };

  private static class MyMapRunner implements
      MapRunnable<IntWritable, ClueWarcRecord, IntWritable, Text> {
    private static final IntWritable KEY = new IntWritable();
    private static final Text VALUE = new Text();

    private int fileno;

    public void configure(JobConf job) {
      String file = job.get("map.input.file");
      fileno = Integer.parseInt(file.substring(file.indexOf("part-") + 5));
    }

    public void run(RecordReader<IntWritable, ClueWarcRecord> input,
        OutputCollector<IntWritable, Text> output, Reporter reporter) throws IOException {
      IntWritable key = new IntWritable();
      ClueWarcRecord value = new ClueWarcRecord();

      long pos = -1;
      long prevPos = -1;

      int prevDocno = 0;

      pos = input.getPos();
      while (input.next(key, value)) {
        if (prevPos != -1 && prevPos != pos) {
          LOG.info("- beginning of block at " + prevPos + ", docno:" + prevDocno + ", file:" + fileno);
          KEY.set(prevDocno);
          VALUE.set(prevPos + "\t" + fileno);
          output.collect(KEY, VALUE);
          reporter.incrCounter(Blocks.Total, 1);
        }

        prevPos = pos;
        pos = input.getPos();
        prevDocno = key.get();
      }
    }
  }

  public ClueWarcForwardIndexBuilder() {}

  public static final String COLLECTION_OPTION = "collection";
  public static final String INDEX_OPTION = "index";

  /**
   * Runs this tool.
   */
  @SuppressWarnings("static-access")
  public int run(String[] args) throws Exception {
    Options options = new Options();
    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("(required) collection path (must be block-compressed SequenceFiles)")
        .create(COLLECTION_OPTION));
    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("(required) output index path").create(INDEX_OPTION));

    CommandLine cmdline;
    CommandLineParser parser = new GnuParser();
    try {
      cmdline = parser.parse(options, args);
    } catch (ParseException exp) {
      System.err.println("Error parsing command line: " + exp.getMessage());
      return -1;
    }

    if (!cmdline.hasOption(COLLECTION_OPTION) || !cmdline.hasOption(INDEX_OPTION)) {
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp(this.getClass().getName(), options);
      ToolRunner.printGenericCommandUsage(System.out);
      return -1;
    }

    JobConf conf = new JobConf(getConf(), ClueWarcForwardIndexBuilder.class);
    FileSystem fs = FileSystem.get(conf);

    String collectionPath = cmdline.getOptionValue(COLLECTION_OPTION);
    String indexFile = cmdline.getOptionValue(INDEX_OPTION);

    LOG.info("Tool name: " + ClueWarcForwardIndexBuilder.class.getSimpleName());
    LOG.info(" - collection path: " + collectionPath);
    LOG.info(" - index file: " + indexFile);
    LOG.info("Note: This tool only works on block-compressed SequenceFiles!");

    Random random = new Random();
    Path outputPath = new Path("tmp-" + ClueWarcForwardIndexBuilder.class.getSimpleName() +
        "-" + random.nextInt(10000));

    conf.setJobName(ClueWarcForwardIndexBuilder.class.getSimpleName() + ":" + collectionPath);

    conf.setNumMapTasks(100);
    conf.setNumReduceTasks(1);

    // Note, we have to add the files one by one, otherwise, SequenceFileInputFormat
    // thinks its a MapFile.
    for (FileStatus status : fs.listStatus(new Path(collectionPath))) {
      FileInputFormat.addInputPath(conf, status.getPath());
    }
    FileOutputFormat.setOutputPath(conf, outputPath);
    FileOutputFormat.setCompressOutput(conf, false);

    conf.setInputFormat(NoSplitSequenceFileInputFormat.class);
    conf.setOutputKeyClass(IntWritable.class);
    conf.setOutputValueClass(Text.class);

    conf.setMapRunnerClass(MyMapRunner.class);
    conf.setReducerClass(IdentityReducer.class);

    // delete the output directory if it exists already
    fs.delete(outputPath, true);

    RunningJob job = JobClient.runJob(conf);

    Counters counters = job.getCounters();
    int blocks = (int) counters.findCounter(Blocks.Total).getCounter();

    LOG.info("number of blocks: " + blocks);

    LOG.info("Writing index file...");
    LineReader reader = new LineReader(fs.open(new Path(outputPath + "/part-00000")));
    FSDataOutputStream out = fs.create(new Path(indexFile), true);

    out.writeUTF(ClueWarcForwardIndex.class.getCanonicalName());
    out.writeUTF(collectionPath);
    out.writeInt(blocks);

    int cnt = 0;
    Text line = new Text();
    while (reader.readLine(line) > 0) {
      String[] arr = line.toString().split("\\s+");

      int docno = Integer.parseInt(arr[0]);
      int offset = Integer.parseInt(arr[1]);
      short fileno = Short.parseShort(arr[2]);

      out.writeInt(docno);
      out.writeInt(offset);
      out.writeShort(fileno);

      cnt++;

      if (cnt % 100000 == 0) {
        LOG.info(cnt + " blocks written");
      }
    }

    reader.close();
    out.close();

    if (cnt != blocks) {
      throw new RuntimeException("Error: mismatch in block count!");
    }

    fs.delete(outputPath, true);
    return 0;
  }

  /**
   * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
   */
  public static void main(String[] args) throws Exception {
    LOG.info("Running " + ClueWarcForwardIndexBuilder.class.getCanonicalName() +
        " with args " + Arrays.toString(args));
    ToolRunner.run(new ClueWarcForwardIndexBuilder(), args);
  }
}