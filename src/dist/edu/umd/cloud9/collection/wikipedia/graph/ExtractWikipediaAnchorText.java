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

package edu.umd.cloud9.collection.wikipedia.graph;

import java.io.IOException;
import java.util.Iterator;
import java.util.Random;

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
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapFileOutputFormat;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Partitioner;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import edu.umd.cloud9.collection.wikipedia.WikipediaPage;
import edu.umd.cloud9.collection.wikipedia.WikipediaPage.Link;
import edu.umd.cloud9.io.map.HMapSIW;
import edu.umd.cloud9.io.pair.PairOfIntString;
import edu.umd.cloud9.io.pair.PairOfStringInt;
import edu.umd.cloud9.io.pair.PairOfStrings;

/**
 * Tool for extracting anchor text out of Wikipedia.
 * 
 * @author Jimmy Lin
 */
public class ExtractWikipediaAnchorText extends Configured implements Tool {
  private static final Logger LOG = Logger.getLogger(ExtractWikipediaAnchorText.class);

  private static enum PageTypes {
    TOTAL, REDIRECT, DISAMBIGUATION, EMPTY, ARTICLE, STUB, NON_ARTICLE
  };

  private static class MyMapper1 extends MapReduceBase implements
      Mapper<IntWritable, WikipediaPage, PairOfStringInt, PairOfStrings> {
    private static final PairOfStringInt KEYPAIR = new PairOfStringInt();
    private static final PairOfStrings VALUEPAIR = new PairOfStrings();

    // Basic algorithm:
    // Emit: key = (link target article name, 0), value = (link target docid, "");
    // Emit: key = (link target article name, 1), value = (src docid, anchor text)
    public void map(IntWritable key, WikipediaPage p,
        OutputCollector<PairOfStringInt, PairOfStrings> output, Reporter reporter)
        throws IOException {
      reporter.incrCounter(PageTypes.TOTAL, 1);

      String title = p.getTitle();

      // This is a caveat and a potential gotcha: Wikipedia article titles are not case sensitive on
      // the initial character, so a link to "commodity" will go to the article titled "Commodity"
      // without any issue. Therefore we need to emit two versions of article titles.

      VALUEPAIR.set(p.getDocid(), "");
      KEYPAIR.set(title, 0);
      output.collect(KEYPAIR, VALUEPAIR);

      String fc = title.substring(0, 1);
      if (fc.matches("[A-Z]")) {
        title = title.replaceFirst(fc, fc.toLowerCase());

        KEYPAIR.set(title, 0);
        output.collect(KEYPAIR, VALUEPAIR);
      }

      if (p.isRedirect()) {
        reporter.incrCounter(PageTypes.REDIRECT, 1);
      } else if (p.isDisambiguation()) {
        reporter.incrCounter(PageTypes.DISAMBIGUATION, 1);
      } else if (p.isEmpty()) {
        reporter.incrCounter(PageTypes.EMPTY, 1);
      } else if (p.isArticle()) {
        reporter.incrCounter(PageTypes.ARTICLE, 1);

        if (p.isStub()) {
          reporter.incrCounter(PageTypes.STUB, 1);
        }
      } else {
        reporter.incrCounter(PageTypes.NON_ARTICLE, 1);
      }

      for (Link link : p.extractLinks()) {
        KEYPAIR.set(link.getTarget(), 1);
        VALUEPAIR.set(p.getDocid(), link.getAnchorText());

        output.collect(KEYPAIR, VALUEPAIR);
      }
    }
  }

  private static class MyReducer1 extends MapReduceBase implements
      Reducer<PairOfStringInt, PairOfStrings, IntWritable, PairOfIntString> {
    private static final IntWritable SRCID = new IntWritable();
    private static final PairOfIntString TARGET_ANCHOR_PAIR = new PairOfIntString();

    private String targetTitle;
    private int targetDocid;

    public void reduce(PairOfStringInt key, Iterator<PairOfStrings> values,
        OutputCollector<IntWritable, PairOfIntString> output, Reporter reporter) throws IOException {

      if (key.getRightElement() == 0) {
        targetTitle = key.getLeftElement();
        targetDocid = Integer.parseInt(values.next().getLeftElement());
      } else {
        if (!key.getLeftElement().equals(targetTitle)) {
          return;
        }

        while (values.hasNext()) {
          PairOfStrings pair = values.next();
          SRCID.set(Integer.parseInt(pair.getLeftElement()));
          TARGET_ANCHOR_PAIR.set(targetDocid, pair.getRightElement());

          output.collect(SRCID, TARGET_ANCHOR_PAIR);
        }
      }
    }
  }

  private static class MyPartitioner1 implements Partitioner<PairOfStringInt, PairOfStrings> {
    public void configure(JobConf job) {}

    public int getPartition(PairOfStringInt key, PairOfStrings value, int numReduceTasks) {
      return (key.getLeftElement().hashCode() & Integer.MAX_VALUE) % numReduceTasks;
    }
  }

  private static class MyMapper2 extends MapReduceBase implements
      Mapper<IntWritable, PairOfIntString, IntWritable, Text> {
    private static final IntWritable KEY = new IntWritable();
    private static final Text VALUE = new Text();

    public void map(IntWritable key, PairOfIntString t, OutputCollector<IntWritable, Text> output,
        Reporter reporter) throws IOException {
      KEY.set(t.getLeftElement());
      VALUE.set(t.getRightElement());

      output.collect(KEY, VALUE);
    }
  }

  private static class MyReducer2 extends MapReduceBase implements
      Reducer<IntWritable, Text, IntWritable, HMapSIW> {
    private static final HMapSIW map = new HMapSIW();

    public void reduce(IntWritable key, Iterator<Text> values,
        OutputCollector<IntWritable, HMapSIW> output, Reporter reporter) throws IOException {
      map.clear();

      Text cur;
      while (values.hasNext()) {
        cur = values.next();

        map.increment(cur.toString());
      }

      output.collect(key, map);
    }
  }

  private static final String INPUT_OPTION = "input";
  private static final String OUTPUT_OPTION = "output";

  @SuppressWarnings("static-access")
  @Override
  public int run(String[] args) throws Exception {
    Options options = new Options();
    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("input").create(INPUT_OPTION));
    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("output for adjacency list").create(OUTPUT_OPTION));

    CommandLine cmdline;
    CommandLineParser parser = new GnuParser();
    try {
      cmdline = parser.parse(options, args);
    } catch (ParseException exp) {
      System.err.println("Error parsing command line: " + exp.getMessage());
      return -1;
    }

    if (!cmdline.hasOption(INPUT_OPTION) || !cmdline.hasOption(OUTPUT_OPTION)) {
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp(this.getClass().getName(), options);
      ToolRunner.printGenericCommandUsage(System.out);
      return -1;
    }

    Random random = new Random();
    String tmp = "tmp-" + this.getClass().getCanonicalName() + "-" + random.nextInt(10000);

    task1(cmdline.getOptionValue(INPUT_OPTION), tmp);
    task2(tmp, cmdline.getOptionValue(OUTPUT_OPTION));

    return 0;
  }

  private void task1(String inputPath, String outputPath) throws IOException {
    LOG.info("Exracting anchor text (phase 1)...");
    LOG.info(" - input: " + inputPath);
    LOG.info(" - output: " + outputPath);

    JobConf conf = new JobConf(getConf(), ExtractWikipediaAnchorText.class);
    conf.setJobName(String.format(
        "ExtractWikipediaAnchorText:phase1[input: %s, output: %s]", inputPath, outputPath));

    // 10 reducers is reasonable.
    conf.setNumReduceTasks(10);

    SequenceFileInputFormat.addInputPath(conf, new Path(inputPath));
    TextOutputFormat.setOutputPath(conf, new Path(outputPath));

    conf.setInputFormat(SequenceFileInputFormat.class);
    conf.setOutputFormat(SequenceFileOutputFormat.class);

    conf.setMapOutputKeyClass(PairOfStringInt.class);
    conf.setMapOutputValueClass(PairOfStrings.class);

    conf.setOutputKeyClass(IntWritable.class);
    conf.setOutputValueClass(PairOfIntString.class);

    conf.setMapperClass(MyMapper1.class);
    conf.setReducerClass(MyReducer1.class);
    conf.setPartitionerClass(MyPartitioner1.class);

    // Delete the output directory if it exists already.
    FileSystem.get(conf).delete(new Path(outputPath), true);

    JobClient.runJob(conf);
  }

  private void task2(String inputPath, String outputPath) throws IOException {
    LOG.info("Exracting anchor text (phase 2)...");
    LOG.info(" - input: " + inputPath);
    LOG.info(" - output: " + outputPath);

    JobConf conf = new JobConf(getConf(), ExtractWikipediaAnchorText.class);
    conf.setJobName(String.format(
        "ExtractWikipediaAnchorText:phase2[input: %s, output: %s]", inputPath, outputPath));

    // Gathers everything together for convenience; feasible for Wikipedia.
    conf.setNumReduceTasks(1);

    TextInputFormat.addInputPath(conf, new Path(inputPath));
    TextOutputFormat.setOutputPath(conf, new Path(outputPath));

    conf.setInputFormat(SequenceFileInputFormat.class);
    conf.setOutputFormat(MapFileOutputFormat.class);

    conf.setMapOutputKeyClass(IntWritable.class);
    conf.setMapOutputValueClass(Text.class);

    conf.setOutputKeyClass(IntWritable.class);
    conf.setOutputValueClass(HMapSIW.class);

    conf.setMapperClass(MyMapper2.class);
    conf.setReducerClass(MyReducer2.class);

    // Delete the output directory if it exists already.
    FileSystem.get(conf).delete(new Path(outputPath), true);

    JobClient.runJob(conf);
    // Clean up intermediate data.
    FileSystem.get(conf).delete(new Path(inputPath), true);
  }

  public ExtractWikipediaAnchorText() {}

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new ExtractWikipediaAnchorText(), args);
    System.exit(res);
  }
}
