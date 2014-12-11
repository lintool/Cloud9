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
import java.net.URI;
import java.util.Arrays;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.lib.NullOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

/**
 * <p>
 * Simple demo program to count the number of records in the ClueWeb09 collection, from either the
 * original source WARC files or repacked SequenceFiles. Sample invocations:
 * </p>
 *
 * <pre>
 * hadoop jar dist/cloud9-X.X.X.jar edu.umd.cloud9.collection.clue.CountClueWarcRecords \
 *  -libjars lib/guava-X.X.X.jar \
 *  -original -path /shared/collections/ClueWeb09/collection.raw/ -segment 1 \
 *  -docnoMapping /shared/collections/ClueWeb09/docno-mapping.dat -countOutput records.txt
 *
 * hadoop jar dist/cloud9-X.X.X.jar edu.umd.cloud9.collection.clue.CountClueWarcRecords \
 *  -libjars lib/guava-X.X.X.jar \
 *  -repacked -path /shared/collections/ClueWeb09/collection.compressed.block/en.01 \
 *  -docnoMapping /shared/collections/ClueWeb09/docno-mapping.dat -countOutput records.txt
 * </pre>
 *
 * @author Jimmy Lin
 */
public class CountClueWarcRecords extends Configured implements Tool {
  private static final Logger LOG = Logger.getLogger(CountClueWarcRecords.class);

  private static enum Records { TOTAL, PAGES };

  private static class MyMapper extends MapReduceBase implements
      Mapper<Writable, ClueWarcRecord, Writable, Text> {
    ClueWarcDocnoMapping docMapping = new ClueWarcDocnoMapping();

    public void configure(JobConf job) {
      try {
        Path[] localFiles = DistributedCache.getLocalCacheFiles(job);
        docMapping.loadMapping(localFiles[0], FileSystem.getLocal(job));
      } catch (Exception e) {
        e.printStackTrace();
        throw new RuntimeException("Error initializing DocnoMapping!");
      }
    }

    public void map(Writable key, ClueWarcRecord doc, OutputCollector<Writable, Text> output,
        Reporter reporter) throws IOException {
      reporter.incrCounter(Records.TOTAL, 1);

      String docid = doc.getHeaderMetadataItem("WARC-TREC-ID");
      int docno = docMapping.getDocno(docid);

      if (docid != null && docno != -1)
        reporter.incrCounter(Records.PAGES, 1);
    }
  }

  public CountClueWarcRecords() {
  }

  public static final String ORIGINAL_OPTION = "original";
  public static final String REPACKED_OPTION = "repacked";
  public static final String PATH_OPTION = "path";
  public static final String MAPPING_OPTION = "docnoMapping";
  public static final String SEGMENT_OPTION = "segment";
  public static final String COUNT_OPTION = "countOutput";

  /**
   * Runs this tool.
   */
  @SuppressWarnings("static-access")
  public int run(String[] args) throws Exception {
    Options options = new Options();
    options.addOption(new Option(ORIGINAL_OPTION, "use original ClueWeb09 distribution"));
    options.addOption(new Option(REPACKED_OPTION, "use repacked SequenceFiles"));

    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("path: base path for 'original', actual path for 'repacked'").create(PATH_OPTION));
    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("DocnoMapping data path").create(MAPPING_OPTION));
    options.addOption(OptionBuilder.withArgName("num").hasArg()
        .withDescription("segment number (required if 'original')").create(SEGMENT_OPTION));
    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("output file to write the number of records").create(COUNT_OPTION));

    CommandLine cmdline;
    CommandLineParser parser = new GnuParser();
    try {
      cmdline = parser.parse(options, args);
    } catch (ParseException exp) {
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp(this.getClass().getName(), options);
      ToolRunner.printGenericCommandUsage(System.out);
      System.err.println("Error parsing command line: " + exp.getMessage());
      return -1;
    }

    boolean repacked;
    if (cmdline.hasOption(REPACKED_OPTION)) {
      repacked = true;
    } else if (cmdline.hasOption(ORIGINAL_OPTION)) {
      repacked = false;
    } else {
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp(this.getClass().getName(), options);
      ToolRunner.printGenericCommandUsage(System.out);
      System.err.println("Expecting either -original or -repacked");
      return -1;
    }

    if (!cmdline.hasOption(PATH_OPTION) || !cmdline.hasOption(MAPPING_OPTION)
        || (!repacked && !cmdline.hasOption(SEGMENT_OPTION))) {
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp(this.getClass().getName(), options);
      ToolRunner.printGenericCommandUsage(System.out);
      return -1;
    }

    String path = cmdline.getOptionValue(PATH_OPTION);
    String mappingFile = cmdline.getOptionValue(MAPPING_OPTION);

    int segment = 1;
    if (!repacked) {
      segment = Integer.parseInt(cmdline.getOptionValue(SEGMENT_OPTION));
    }

    LOG.info("Tool name: " + CountClueWarcRecords.class.getSimpleName());
    LOG.info(" - repacked: " + repacked);
    LOG.info(" - path: " + path);
    LOG.info(" - mapping file: " + mappingFile);
    if (!repacked) {
      LOG.info(" - segment number: " + segment);
    }

    FileSystem fs = FileSystem.get(getConf());
    int mapTasks = 10;

    JobConf conf = new JobConf(getConf(), CountClueWarcRecords.class);
    conf.setJobName(CountClueWarcRecords.class.getSimpleName()
        + (repacked ? ":" + path : ":segment" + segment));

    conf.setNumMapTasks(mapTasks);
    conf.setNumReduceTasks(0);

    if (repacked) {
      // Note, we have to add the files one by one, otherwise, SequenceFileInputFormat
      // thinks its a MapFile.
      for (FileStatus status : fs.listStatus(new Path(path))) {
        FileInputFormat.addInputPath(conf, status.getPath());
      }
    } else {
      ClueCollectionPathConstants.addEnglishCollectionPart(conf, path, segment);
    }

    DistributedCache.addCacheFile(new URI(mappingFile), conf);

    if (repacked) {
      conf.setInputFormat(SequenceFileInputFormat.class);
    } else {
      conf.setInputFormat(ClueWarcInputFormat.class);
    }

    conf.setOutputFormat(NullOutputFormat.class);
    conf.setMapperClass(MyMapper.class);

    RunningJob job = JobClient.runJob(conf);
    Counters counters = job.getCounters();
    int numDocs = (int) counters.findCounter(Records.PAGES).getCounter();

    LOG.info("Read " + numDocs + " docs.");

    if (cmdline.hasOption(COUNT_OPTION)) {
      String f = cmdline.getOptionValue(COUNT_OPTION);
      FSDataOutputStream out = fs.create(new Path(f));
      out.write(new Integer(numDocs).toString().getBytes());
      out.close();
    }

    return 0;
  }

  /**
   * Dispatches command-line arguments to the tool via the <code>ToolRunner</code>.
   */
  public static void main(String[] args) throws Exception {
    LOG.info("Running " + CountClueWarcRecords.class.getCanonicalName() + " with args "
        + Arrays.toString(args));
    ToolRunner.run(new CountClueWarcRecords(), args);
  }
}
