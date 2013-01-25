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
import java.util.Iterator;
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
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

/**
 * Tool for building the mapping between Wikipedia internal ids (docids) and sequentially-numbered
 * ints (docnos).
 *
 * @author Jimmy Lin
 * @author Peter Exner
 */
public class BuildWikipediaDocnoMapping extends Configured implements Tool {
  private static final Logger LOG = Logger.getLogger(BuildWikipediaDocnoMapping.class);

  private static enum PageTypes {
    TOTAL, REDIRECT, DISAMBIGUATION, EMPTY, ARTICLE, STUB, NON_ARTICLE, OTHER
  };

  private static class MyMapper extends MapReduceBase implements
      Mapper<LongWritable, WikipediaPage, IntWritable, IntWritable> {

    private final static IntWritable keyOut = new IntWritable();
    private final static IntWritable valOut = new IntWritable(1);

    private boolean keepAll;
    
    public void configure(JobConf job) {
      keepAll = job.getBoolean(KEEP_ALL_OPTION, false);
    }

    public void map(LongWritable key, WikipediaPage p,
        OutputCollector<IntWritable, IntWritable> output, Reporter reporter) throws IOException {
      reporter.incrCounter(PageTypes.TOTAL, 1);

      // If we're keeping all pages, don't bother checking.
      if (keepAll) {
        keyOut.set(Integer.parseInt(p.getDocid()));
        output.collect(keyOut, valOut);
        return;
      }
      
      if (p.isRedirect()) {
        reporter.incrCounter(PageTypes.REDIRECT, 1);
      } else if (p.isEmpty()) {
        reporter.incrCounter(PageTypes.EMPTY, 1);
      } else if (p.isDisambiguation()) {
        reporter.incrCounter(PageTypes.DISAMBIGUATION, 1);
      } else if (p.isArticle()) {
        // heuristic: potentially template or stub article
        if (p.getTitle().length() > 0.3*p.getContent().length()) {
          reporter.incrCounter(PageTypes.OTHER, 1);
          return;
        }

        reporter.incrCounter(PageTypes.ARTICLE, 1);

        if (p.isStub()) {
          reporter.incrCounter(PageTypes.STUB, 1);
        }
        

        keyOut.set(Integer.parseInt(p.getDocid()));
        output.collect(keyOut, valOut);
      } else {
        reporter.incrCounter(PageTypes.NON_ARTICLE, 1);
      }
    }
  }

  private static class MyReducer extends MapReduceBase implements
      Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {

    private final static IntWritable cnt = new IntWritable(1);

    public void reduce(IntWritable key, Iterator<IntWritable> values,
        OutputCollector<IntWritable, IntWritable> output, Reporter reporter) throws IOException {
      output.collect(key, cnt);

      cnt.set(cnt.get() + 1);
    }
  }

  public static final String INPUT_OPTION = "input";
  public static final String OUTPUT_PATH_OPTION = "output_path";
  public static final String OUTPUT_FILE_OPTION = "output_file";
  public static final String KEEP_ALL_OPTION = "keep_all";
  public static final String LANGUAGE_OPTION = "wiki_language";

  @SuppressWarnings("static-access")
  @Override
  public int run(String[] args) throws Exception {
    Options options = new Options();
    options.addOption(OptionBuilder.withArgName("path")
        .hasArg().withDescription("XML dump file").create(INPUT_OPTION));
    options.addOption(OptionBuilder.withArgName("path")
        .hasArg().withDescription("tmp output directory").create(OUTPUT_PATH_OPTION));
    options.addOption(OptionBuilder.withArgName("path")
        .hasArg().withDescription("output file").create(OUTPUT_FILE_OPTION));
    options.addOption(OptionBuilder.withArgName("en|sv|de|cs|es|zh|ar|tr").hasArg()
        .withDescription("two-letter language code").create(LANGUAGE_OPTION));
    options.addOption(KEEP_ALL_OPTION, false, "keep all pages");

    CommandLine cmdline;
    CommandLineParser parser = new GnuParser();
    try {
      cmdline = parser.parse(options, args);
    } catch (ParseException exp) {
      System.err.println("Error parsing command line: " + exp.getMessage());
      return -1;
    }

    if (!cmdline.hasOption(INPUT_OPTION) || !cmdline.hasOption(OUTPUT_PATH_OPTION)
        || !cmdline.hasOption(OUTPUT_FILE_OPTION)) {
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp(this.getClass().getName(), options);
      ToolRunner.printGenericCommandUsage(System.out);
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
    
    String inputPath = cmdline.getOptionValue(INPUT_OPTION);
    String outputPath = cmdline.getOptionValue(OUTPUT_PATH_OPTION);
    String outputFile = cmdline.getOptionValue(OUTPUT_FILE_OPTION);
    boolean keepAll = cmdline.hasOption(KEEP_ALL_OPTION);
    

    LOG.info("Tool name: " + this.getClass().getName());
    LOG.info(" - input: " + inputPath);
    LOG.info(" - output path: " + outputPath);
    LOG.info(" - output file: " + outputFile);
    LOG.info(" - keep all pages: " + keepAll);
    LOG.info(" - language: " + language);

    JobConf conf = new JobConf(getConf(), BuildWikipediaDocnoMapping.class);
    conf.setJobName(String.format("BuildWikipediaDocnoMapping[%s: %s, %s: %s, %s: %s]", INPUT_OPTION,
        inputPath, OUTPUT_FILE_OPTION, outputFile, LANGUAGE_OPTION, language));

    conf.setBoolean(KEEP_ALL_OPTION, keepAll);
    if(language != null){
      conf.set("wiki.language", language);
    }
    conf.setNumReduceTasks(1);

    FileInputFormat.setInputPaths(conf, new Path(inputPath));
    FileOutputFormat.setOutputPath(conf, new Path(outputPath));
    FileOutputFormat.setCompressOutput(conf, false);

    conf.setInputFormat(WikipediaPageInputFormat.class);
    conf.setOutputKeyClass(IntWritable.class);
    conf.setOutputValueClass(IntWritable.class);
    conf.setOutputFormat(TextOutputFormat.class);

    conf.setMapperClass(MyMapper.class);
    conf.setReducerClass(MyReducer.class);

    // Delete the output directory if it exists already.
    FileSystem.get(conf).delete(new Path(outputPath), true);

    RunningJob job = JobClient.runJob(conf);
    Counters c = job.getCounters();
    long cnt = keepAll ? c.getCounter(PageTypes.TOTAL) : c.getCounter(PageTypes.ARTICLE);

    WikipediaDocnoMapping.writeDocnoMappingData(FileSystem.get(conf),
        outputPath + "/part-00000", (int) cnt, outputFile);

    return 0;
  }

  public BuildWikipediaDocnoMapping() {}

  public static void main(String[] args) throws Exception {
    ToolRunner.run(new BuildWikipediaDocnoMapping(), args);
  }
}
