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
 * Tool for building the mapping between Wikipedia internal ids (docids) and
 * sequentially-numbered ints (docnos).
 *
 * @author Jimmy Lin
 */
@SuppressWarnings("deprecation")
public class BuildWikipediaDocnoMapping extends Configured implements Tool {
	private static final Logger LOG = Logger.getLogger(BuildWikipediaDocnoMapping.class);

	private static enum PageTypes {
		TOTAL, REDIRECT, DISAMBIGUATION, EMPTY, ARTICLE, STUB, NON_ARTICLE
	};

	private static class MyMapper extends MapReduceBase implements
			Mapper<LongWritable, WikipediaPage, IntWritable, IntWritable> {

		private final static IntWritable keyOut = new IntWritable();
		private final static IntWritable valOut = new IntWritable(1);

		public void map(LongWritable key, WikipediaPage p,
				OutputCollector<IntWritable, IntWritable> output, Reporter reporter)
				throws IOException {
			reporter.incrCounter(PageTypes.TOTAL, 1);

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
				OutputCollector<IntWritable, IntWritable> output, Reporter reporter)
				throws IOException {
			output.collect(key, cnt);

			cnt.set(cnt.get() + 1);
		}
	}

  private static final String INPUT_OPTION = "input";
  private static final String OUTPUT_PATH_OPTION = "output_path";
  private static final String OUTPUT_FILE_OPTION = "output_file";

	@SuppressWarnings("static-access") @Override
	public int run(String[] args) throws Exception {
    Options options = new Options();
    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("XML dump file").create(INPUT_OPTION));
    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("tmp output directory").create(OUTPUT_PATH_OPTION));
    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("output file").create(OUTPUT_FILE_OPTION));

    CommandLine cmdline;
    CommandLineParser parser = new GnuParser();
    try {
      cmdline = parser.parse(options, args);
    } catch (ParseException exp) {
      System.err.println("Error parsing command line: " + exp.getMessage());
      return -1;
    }

    if (!cmdline.hasOption(INPUT_OPTION) || !cmdline.hasOption(OUTPUT_PATH_OPTION) ||
        !cmdline.hasOption(OUTPUT_FILE_OPTION)) {
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp(this.getClass().getName(), options);
      ToolRunner.printGenericCommandUsage(System.out);
      return -1;
    }

    String inputPath = cmdline.getOptionValue(INPUT_OPTION);
		String outputPath = cmdline.getOptionValue(OUTPUT_PATH_OPTION);
		String outputFile = cmdline.getOptionValue(OUTPUT_FILE_OPTION);

		LOG.info("Tool name: " + this.getClass().getName());
		LOG.info(" - input: " + inputPath);
		LOG.info(" - output path: " + outputPath);
		LOG.info(" - output file: " + outputFile);

		JobConf conf = new JobConf(getConf(), BuildWikipediaDocnoMapping.class);
    conf.setJobName(String.format("BuildWikipediaDocnoMapping[%s: %s, %s: %s]",
        INPUT_OPTION, inputPath, OUTPUT_FILE_OPTION, outputFile));

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
		long cnt = c.getCounter(PageTypes.ARTICLE);

		WikipediaDocnoMapping.writeDocnoMappingData(outputPath + "/part-00000", (int) cnt, outputFile);

		return 0;
	}

	public BuildWikipediaDocnoMapping() {}

	public static void main(String[] args) throws Exception {
		ToolRunner.run(new BuildWikipediaDocnoMapping(), args);
	}
}
