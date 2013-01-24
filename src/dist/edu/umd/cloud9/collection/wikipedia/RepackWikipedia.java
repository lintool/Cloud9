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

package edu.umd.cloud9.collection.wikipedia;

import java.io.IOException;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
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
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import edu.umd.cloud9.collection.wikipedia.language.WikipediaPageFactory;

/**
 * Tool for repacking Wikipedia XML dumps into <code>SequenceFiles</code>.
 *
 * @author Jimmy Lin
 * @author Peter Exner
 */
public class RepackWikipedia extends Configured implements Tool {
  private static final Logger LOG = Logger.getLogger(RepackWikipedia.class);

  private static enum Records { TOTAL };

  private static class MyMapper extends MapReduceBase implements
  Mapper<LongWritable, WikipediaPage, IntWritable, WikipediaPage> {

    private static final IntWritable docno = new IntWritable();
    private static final WikipediaDocnoMapping docnoMapping = new WikipediaDocnoMapping();

    public void configure(JobConf job) {
      try {
        Path p = new Path(job.get(DOCNO_MAPPING_FIELD));
        LOG.info("Loading docno mapping: " + p);

        FileSystem fs = FileSystem.get(job);
        if (!fs.exists(p)) {
          throw new RuntimeException(p + " does not exist!");
        }

        docnoMapping.loadMapping(p, fs);
      } catch (Exception e) {
        throw new RuntimeException("Error loading docno mapping data file!");
      }
    }

    public void map(LongWritable key, WikipediaPage doc,
        OutputCollector<IntWritable, WikipediaPage> output, Reporter reporter) throws IOException {
      reporter.incrCounter(Records.TOTAL, 1);
      String id = doc.getDocid();
      if (id != null) {
        // We're going to discard pages that aren't in the docno mapping.
        int n = docnoMapping.getDocno(id);
        if (n >= 0) {
          docno.set(n);
          
          output.collect(docno, doc);
        }
      }
    }
  }

  private static final String DOCNO_MAPPING_FIELD = "DocnoMappingDataFile";

  private static final String INPUT_OPTION = "input";
  private static final String OUTPUT_OPTION = "output";
  private static final String MAPPING_FILE_OPTION = "mapping_file";
  private static final String COMPRESSION_TYPE_OPTION = "compression_type";
  private static final String LANGUAGE_OPTION = "wiki_language";

  @SuppressWarnings("static-access") @Override
  public int run(String[] args) throws Exception {
    Options options = new Options();
    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("XML dump file").create(INPUT_OPTION));
    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("output location").create(OUTPUT_OPTION));
    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("mapping file").create(MAPPING_FILE_OPTION));
    options.addOption(OptionBuilder.withArgName("block|record|none").hasArg()
        .withDescription("compression type").create(COMPRESSION_TYPE_OPTION));
    options.addOption(OptionBuilder.withArgName("en|sv|de").hasArg()
        .withDescription("two-letter language code").create(LANGUAGE_OPTION));

    CommandLine cmdline;
    CommandLineParser parser = new GnuParser();
    try {
      cmdline = parser.parse(options, args);
    } catch (ParseException exp) {
      System.err.println("Error parsing command line: " + exp.getMessage());
      return -1;
    }

    if (!cmdline.hasOption(INPUT_OPTION) || !cmdline.hasOption(OUTPUT_OPTION) ||
        !cmdline.hasOption(MAPPING_FILE_OPTION) || !cmdline.hasOption(COMPRESSION_TYPE_OPTION)) {
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp(this.getClass().getName(), options);
      ToolRunner.printGenericCommandUsage(System.out);
      return -1;
    }

    String inputPath = cmdline.getOptionValue(INPUT_OPTION);
    String outputPath = cmdline.getOptionValue(OUTPUT_OPTION);
    String mappingFile = cmdline.getOptionValue(MAPPING_FILE_OPTION);
    String compressionType = cmdline.getOptionValue(COMPRESSION_TYPE_OPTION);

    if (!"block".equals(compressionType) && !"record".equals(compressionType) && !"none".equals(compressionType)) {
      System.err.println("Error: \"" + compressionType + "\" unknown compression type!");
      return -1;
    }

    String language = null;
    if (cmdline.hasOption(LANGUAGE_OPTION)) {
      language = cmdline.getOptionValue(LANGUAGE_OPTION);
      if(language.length()!=2){
        System.err.println("Error: \"" + language + "\" unknown language!");
        return -1;
      }
    }

    // this is the default block size
    int blocksize = 1000000;

    JobConf conf = new JobConf(getConf(), RepackWikipedia.class);
    conf.setJobName(String.format("RepackWikipedia[%s: %s, %s: %s, %s: %s, %s: %s]",
        INPUT_OPTION, inputPath, OUTPUT_OPTION, outputPath, COMPRESSION_TYPE_OPTION, compressionType, LANGUAGE_OPTION, language));

    conf.set(DOCNO_MAPPING_FIELD, mappingFile);

    LOG.info("Tool name: " + this.getClass().getName());
    LOG.info(" - XML dump file: " + inputPath);
    LOG.info(" - output path: " + outputPath);
    LOG.info(" - docno mapping data file: " + mappingFile);
    LOG.info(" - compression type: " + compressionType);
    LOG.info(" - language: " + language);

    if ("block".equals(compressionType)) {
      LOG.info(" - block size: " + blocksize);
    }

    int mapTasks = 10;

    conf.setNumMapTasks(mapTasks);
    conf.setNumReduceTasks(0);

    SequenceFileInputFormat.addInputPath(conf, new Path(inputPath));
    SequenceFileOutputFormat.setOutputPath(conf, new Path(outputPath));

    if ("none".equals(compressionType)) {
      SequenceFileOutputFormat.setCompressOutput(conf, false);
    } else {
      SequenceFileOutputFormat.setCompressOutput(conf, true);

      if ("record".equals(compressionType)) {
        SequenceFileOutputFormat.setOutputCompressionType(conf, SequenceFile.CompressionType.RECORD);
      } else {
        SequenceFileOutputFormat.setOutputCompressionType(conf, SequenceFile.CompressionType.BLOCK);
        conf.setInt("io.seqfile.compress.blocksize", blocksize);
      }
    }

    if(language != null){
      conf.set("wiki.language", language);
    }

    conf.setInputFormat(WikipediaPageInputFormat.class);
    conf.setOutputFormat(SequenceFileOutputFormat.class);
    conf.setOutputKeyClass(IntWritable.class);
    conf.setOutputValueClass(WikipediaPageFactory.getWikipediaPageClass(language));

    conf.setMapperClass(MyMapper.class);

    // Delete the output directory if it exists already.
    FileSystem.get(conf).delete(new Path(outputPath), true);

    JobClient.runJob(conf);

    return 0;
  }

  public RepackWikipedia() {}

  public static void main(String[] args) throws Exception {
    ToolRunner.run(new RepackWikipedia(), args);
  }
}
