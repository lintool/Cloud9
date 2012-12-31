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

package edu.umd.cloud9.collection.trec;

import java.io.IOException;
import java.net.URI;
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
 * Tool for building a document forward index for TREC collections. Run without any arguments
 * for help. The guava jar must be included using {@code -libjar} for this tool to run.
 * Sample invocation:
 *
 * <pre>
 * hadoop jar cloud9.jar edu.umd.cloud9.collection.trec.TrecForwardIndexBuilder \
 *  -libjars guava.jar \
 *  -collection /shared/collections/trec/trec4-5_noCRFR.xml \
 *  -docnoMapping /shared/collections/trec/docno-mapping.dat \
 *  -index findex.dat
 * </pre>
 *
 * @author Jimmy Lin
 */
public class TrecForwardIndexBuilder extends Configured implements Tool {
  private static final Logger LOG = Logger.getLogger(TrecForwardIndexBuilder.class);
  private static final Random random = new Random();
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

  public TrecForwardIndexBuilder() {}

  public static final String COLLECTION_OPTION = "collection";
  public static final String INDEX_OPTION = "index";
  public static final String MAPPING_OPTION = "docnoMapping";

  /**
   * Runs this tool.
   */
  @SuppressWarnings("static-access")
  public int run(String[] args) throws Exception {
    Options options = new Options();
    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("(required) collection path").create(COLLECTION_OPTION));
    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("(required) output index path").create(INDEX_OPTION));
    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("(required) DocnoMapping data").create(MAPPING_OPTION));

    CommandLine cmdline;
    CommandLineParser parser = new GnuParser();
    try {
      cmdline = parser.parse(options, args);
    } catch (ParseException exp) {
      System.err.println("Error parsing command line: " + exp.getMessage());
      return -1;
    }

    if (!cmdline.hasOption(COLLECTION_OPTION) || !cmdline.hasOption(INDEX_OPTION) ||
        !cmdline.hasOption(MAPPING_OPTION)) {
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp(this.getClass().getName(), options);
      ToolRunner.printGenericCommandUsage(System.out);
      return -1;
    }

    String collectionPath = cmdline.getOptionValue(COLLECTION_OPTION);
    String indexFile = cmdline.getOptionValue(INDEX_OPTION);
    String mappingFile = cmdline.getOptionValue(MAPPING_OPTION);
    String tmpDir = "tmp-" + TrecForwardIndexBuilder.class.getSimpleName() + "-"
        + random.nextInt(10000);

    Job job = new Job(getConf(),
        TrecForwardIndexBuilder.class.getSimpleName() + ":" + collectionPath);
    job.setJarByClass(TrecForwardIndexBuilder.class);
    FileSystem fs = FileSystem.get(getConf());

    LOG.info("Tool name: " + TrecForwardIndexBuilder.class.getSimpleName());
    LOG.info(" - collection path: " + collectionPath);
    LOG.info(" - index file: " + indexFile);
    LOG.info(" - DocnoMapping file: " + mappingFile);
    LOG.info(" - temp output directory: " + tmpDir);

    job.setNumReduceTasks(1);

    if (job.getConfiguration().get("mapred.job.tracker").equals("local")) {
      job.getConfiguration().set(DOCNO_MAPPING_FILE_PROPERTY, mappingFile);
    } else {
      DistributedCache.addCacheFile(new URI(mappingFile), job.getConfiguration());
    }

    FileInputFormat.setInputPaths(job, new Path(collectionPath));
    FileOutputFormat.setOutputPath(job, new Path(tmpDir));
    FileOutputFormat.setCompressOutput(job, false);

    job.setInputFormatClass(TrecDocumentInputFormat.class);
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(Text.class);

    job.setMapperClass(MyMapper.class);

    // delete the output directory if it exists already
    FileSystem.get(getConf()).delete(new Path(tmpDir), true);

    job.waitForCompletion(true);
    Counters counters = job.getCounters();
    int numDocs = (int) counters.findCounter(Count.DOCS).getValue();

    String inputFile = tmpDir + "/" + "part-r-00000";

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

    fs.delete(new Path(tmpDir), true);

    return 0;
  }

  /**
   * Dispatches command-line arguments to the tool via the <code>ToolRunner</code>.
   */
  public static void main(String[] args) throws Exception {
    LOG.info("Running " + TrecForwardIndexBuilder.class.getCanonicalName() +
        " with args " + Arrays.toString(args));
    ToolRunner.run(new Configuration(), new TrecForwardIndexBuilder(), args);
  }
}
