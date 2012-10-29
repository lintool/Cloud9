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
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

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
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Partitioner;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import edu.umd.cloud9.io.pair.PairOfStringInt;

/**
 * Tool for extracting the link graph out of Wikipedia. Sample invocation:
 *
 * @author Jimmy Lin
 */
public class BuildWikipediaLinkGraph extends Configured implements Tool {
	private static final Logger LOG = Logger.getLogger(BuildWikipediaLinkGraph.class);

	private static enum PageTypes { TOTAL, REDIRECT, DISAMBIGUATION, EMPTY, ARTICLE, STUB, NON_ARTICLE };

	private static class MyMapper1 extends MapReduceBase implements
			Mapper<IntWritable, WikipediaPage, PairOfStringInt, Text> {
		private static Text text = new Text();
		private static PairOfStringInt pair = new PairOfStringInt();

		public void map(IntWritable key, WikipediaPage p,
				OutputCollector<PairOfStringInt, Text> output, Reporter reporter) throws IOException {
			reporter.incrCounter(PageTypes.TOTAL, 1);

			String title = p.getTitle();

			// This is a caveat and a potential gotcha: Wikipedia article titles
			// are not case sensitive on the initial character, so a link to
			// "commodity" will go to the article titled "Commodity" without any
			// issue. Therefore we need to emit two versions of article titles.
			text.set(p.getDocid());
			pair.set(title, 0);
			output.collect(pair, text);

			String fc = title.substring(0, 1);
			if (fc.matches("[A-Z]")) {
				title = title.replaceFirst(fc, fc.toLowerCase());

				pair.set(title, 0);
				output.collect(pair, text);
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

			for (String t : p.extractLinkDestinations()) {
				pair.set(t, 1);
				text.set(p.getDocid());

				output.collect(pair, text);
			}
		}
	}

	private static class MyReducer1 extends MapReduceBase implements
			Reducer<PairOfStringInt, Text, IntWritable, IntWritable> {

		private static final IntWritable finalSrc = new IntWritable();
		private static final IntWritable finalDest = new IntWritable();

		private static String curArticle;
		private static int curDocid;

		public void reduce(PairOfStringInt key, Iterator<Text> values,
				OutputCollector<IntWritable, IntWritable> output, Reporter reporter)
				throws IOException {

			if (key.getRightElement() == 0) {
				// We want to emit a placeholder in case this is a dangling node.
				curArticle = key.getLeftElement();
				curDocid = Integer.parseInt(values.next().toString());

				finalSrc.set(curDocid);
				finalDest.set(curDocid);
				output.collect(finalSrc, finalDest);
			} else {
				if (!key.getLeftElement().equals(curArticle)) {
					return;
				}

				while (values.hasNext()) {
					finalSrc.set(Integer.parseInt(values.next().toString()));
					finalDest.set(curDocid);
					output.collect(finalSrc, finalDest);
				}
			}
		}
	}

	private static class MyPartitioner1 implements Partitioner<PairOfStringInt, Text> {
		public void configure(JobConf job) {}

		public int getPartition(PairOfStringInt key, Text value, int numReduceTasks) {
			return (key.getLeftElement().hashCode() & Integer.MAX_VALUE) % numReduceTasks;
		}
	}

	private static class MyMapper2 extends MapReduceBase implements
			Mapper<LongWritable, Text, IntWritable, IntWritable> {

		private static IntWritable keyOut = new IntWritable();
		private static IntWritable valOut = new IntWritable();

		public void map(LongWritable key, Text t, OutputCollector<IntWritable, IntWritable> output,
				Reporter reporter) throws IOException {

			String[] arr = t.toString().split("\\s+");

			keyOut.set(Integer.parseInt(arr[0]));
			valOut.set(Integer.parseInt(arr[1]));

			output.collect(keyOut, valOut);
		}
	}

	private static class MyReducer2 extends MapReduceBase implements
			Reducer<IntWritable, IntWritable, IntWritable, Text> {

		private final static Text text = new Text();

		public void reduce(IntWritable key, Iterator<IntWritable> values,
				OutputCollector<IntWritable, Text> output, Reporter reporter) throws IOException {

			StringBuilder sb = new StringBuilder();
			Set<Integer> set = new HashSet<Integer>();

			IntWritable cur;
			while (values.hasNext()) {
				cur = values.next();

				if (cur.get() == key.get()) { continue;	}
				if (set.contains(cur.get())) { continue; }  // Keep only one link to target.

				set.add(cur.get());

				sb.append(cur.get());
				sb.append("\t");
			}

			text.set(sb.toString());
			output.collect(key, text);
		}
	}

  private static final String INPUT_OPTION = "input";
  private static final String EDGES_OUTPUT_OPTION = "edges_output";
  private static final String ADJ_OUTPUT_OPTION = "adjacency_list_output";
  private static final String NUM_PARTITIONS_OPTION = "num_partitions";

  @SuppressWarnings("static-access") @Override
	public int run(String[] args) throws Exception {
    Options options = new Options();
    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("input").create(INPUT_OPTION));
    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("output for edges").create(EDGES_OUTPUT_OPTION));
    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("output for adjacency list").create(ADJ_OUTPUT_OPTION));
    options.addOption(OptionBuilder.withArgName("num").hasArg()
        .withDescription("number of partitions").create(NUM_PARTITIONS_OPTION));

    CommandLine cmdline;
    CommandLineParser parser = new GnuParser();
    try {
      cmdline = parser.parse(options, args);
    } catch (ParseException exp) {
      System.err.println("Error parsing command line: " + exp.getMessage());
      return -1;
    }

    if (!cmdline.hasOption(INPUT_OPTION) || !cmdline.hasOption(EDGES_OUTPUT_OPTION)
        || !cmdline.hasOption(ADJ_OUTPUT_OPTION) || !cmdline.hasOption(NUM_PARTITIONS_OPTION)) {
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp(this.getClass().getName(), options);
      ToolRunner.printGenericCommandUsage(System.out);
      return -1;
    }

		int numPartitions = Integer.parseInt(cmdline.getOptionValue(NUM_PARTITIONS_OPTION));

		task1(cmdline.getOptionValue(INPUT_OPTION), cmdline.getOptionValue(EDGES_OUTPUT_OPTION), numPartitions);
		task2(cmdline.getOptionValue(EDGES_OUTPUT_OPTION), cmdline.getOptionValue(ADJ_OUTPUT_OPTION), numPartitions);

		return 0;
	}

	private void task1(String inputPath, String outputPath, int partitions) throws IOException {
		LOG.info("Exracting edges...");
		LOG.info(" - input: " + inputPath);
		LOG.info(" - output: " + outputPath);

		JobConf conf = new JobConf(getConf(), BuildWikipediaLinkGraph.class);
		conf.setJobName(String.format("BuildWikipediaLinkGraph:Edges[input: %s, output: %s, num_partitions: %d]",
		    inputPath, outputPath, partitions));

		conf.setNumReduceTasks(partitions);

		SequenceFileInputFormat.addInputPath(conf, new Path(inputPath));
		TextOutputFormat.setOutputPath(conf, new Path(outputPath));

		conf.setInputFormat(SequenceFileInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		conf.setMapOutputKeyClass(PairOfStringInt.class);
		conf.setMapOutputValueClass(Text.class);

		conf.setOutputKeyClass(IntWritable.class);
		conf.setOutputValueClass(IntWritable.class);

		conf.setMapperClass(MyMapper1.class);
		conf.setReducerClass(MyReducer1.class);
		conf.setPartitionerClass(MyPartitioner1.class);

		// Delete the output directory if it exists already.
		FileSystem.get(conf).delete(new Path(outputPath), true);

		JobClient.runJob(conf);
	}

	private void task2(String inputPath, String outputPath, int partitions) throws IOException {
		LOG.info("Building adjacency lists...");
		LOG.info(" - input: " + inputPath);
		LOG.info(" - output: " + outputPath);

		JobConf conf = new JobConf(getConf(), BuildWikipediaLinkGraph.class);
		conf.setJobName(String.format("BuildWikipediaLinkGraph:AdjacencyList[input: %s, output: %s, num_partitions: %d]",
        inputPath, outputPath, partitions));

		conf.setNumReduceTasks(partitions);

		TextInputFormat.addInputPath(conf, new Path(inputPath));
		TextOutputFormat.setOutputPath(conf, new Path(outputPath));

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		conf.setMapOutputKeyClass(IntWritable.class);
		conf.setMapOutputValueClass(IntWritable.class);

		conf.setOutputKeyClass(IntWritable.class);
		conf.setOutputValueClass(Text.class);

		conf.setMapperClass(MyMapper2.class);
		conf.setReducerClass(MyReducer2.class);

		// Delete the output directory if it exists already.
		FileSystem.get(conf).delete(new Path(outputPath), true);

		JobClient.runJob(conf);
	}

	public BuildWikipediaLinkGraph() {}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new BuildWikipediaLinkGraph(), args);
		System.exit(res);
	}
}
