package edu.umd.cloud9.collection.wikipedia;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
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

import edu.umd.cloud9.io.PairOfStringInt;

public class BuildWikipediaLinkGraph extends Configured implements Tool {

	private static final Logger sLogger = Logger.getLogger(BuildWikipediaLinkGraph.class);

	private static enum PageTypes {
		TOTAL, REDIRECT, DISAMBIGUATION, EMPTY, ARTICLE, STUB
	};

	private static class MyMapper1 extends MapReduceBase implements
			Mapper<IntWritable, WikipediaPage, PairOfStringInt, Text> {

		private static Text sText = new Text();
		private static PairOfStringInt sPair = new PairOfStringInt();

		public void map(IntWritable key, WikipediaPage p,
				OutputCollector<PairOfStringInt, Text> output, Reporter reporter)
				throws IOException {
			reporter.incrCounter(PageTypes.TOTAL, 1);

			// This is a caveat and a potential gotcha: Wikipedia article titles
			// are not case sensitive on the initial character, so a link to
			// "commodity" will go to the article titled "Commodity" without any
			// issue. Therefore we need to emit two versions of article titles.

			sText.set(p.getDocid());

			String title = p.getTitle();

			sPair.set(title, 0);
			output.collect(sPair, sText);

			String fc = title.substring(0, 1);

			if (fc.matches("[A-Z]")) {
				title = title.replaceFirst(fc, fc.toLowerCase());

				sPair.set(title, 0);
				output.collect(sPair, sText);
			}

			if (p.isRedirect()) {
				reporter.incrCounter(PageTypes.REDIRECT, 1);
			} else if (p.isDisambiguation()) {
				reporter.incrCounter(PageTypes.DISAMBIGUATION, 1);
			} else if (p.isEmpty()) {
				reporter.incrCounter(PageTypes.EMPTY, 1);
			} else {
				reporter.incrCounter(PageTypes.ARTICLE, 1);

				if (p.isStub()) {
					reporter.incrCounter(PageTypes.STUB, 1);
				}
			}

			for (String t : p.extractLinkDestinations()) {
				sText.set(t);

				sPair.set(t, 1);
				sText.set(p.getDocid());

				output.collect(sPair, sText);
			}

		}
	}

	private static class MyReducer1 extends MapReduceBase implements
			Reducer<PairOfStringInt, Text, IntWritable, IntWritable> {

		private final static IntWritable sFinalSrc = new IntWritable();
		private final static IntWritable sFinalDest = new IntWritable();

		private static String sCurrentArticle;
		private static int sCurrentDocid;

		public void reduce(PairOfStringInt key, Iterator<Text> values,
				OutputCollector<IntWritable, IntWritable> output, Reporter reporter)
				throws IOException {

			if (key.getRightElement() == 0) {
				// we want to emit a placeholder in case this is a dangling node
				sCurrentArticle = key.getLeftElement();
				sCurrentDocid = Integer.parseInt(values.next().toString());

				sFinalSrc.set(sCurrentDocid);
				sFinalDest.set(sCurrentDocid);
				output.collect(sFinalSrc, sFinalDest);
			} else {
				if (!key.getLeftElement().equals(sCurrentArticle))
					return;

				while (values.hasNext()) {
					sFinalSrc.set(Integer.parseInt(values.next().toString()));
					sFinalDest.set(sCurrentDocid);
					output.collect(sFinalSrc, sFinalDest);
				}
			}
		}
	}

	private static class MyPartitioner1 implements Partitioner<PairOfStringInt, Text> {
		public void configure(JobConf job) {
		}

		public int getPartition(PairOfStringInt key, Text value, int numReduceTasks) {
			return (key.getLeftElement().hashCode() & Integer.MAX_VALUE) % numReduceTasks;
		}
	}

	private static class MyMapper2 extends MapReduceBase implements
			Mapper<LongWritable, Text, IntWritable, IntWritable> {

		private static IntWritable sOutKey = new IntWritable();
		private static IntWritable sOutValue = new IntWritable();

		public void map(LongWritable key, Text t, OutputCollector<IntWritable, IntWritable> output,
				Reporter reporter) throws IOException {

			String[] arr = t.toString().split("\\s+");

			sOutKey.set(Integer.parseInt(arr[0]));
			sOutValue.set(Integer.parseInt(arr[1]));

			output.collect(sOutKey, sOutValue);
		}
	}

	private static class MyReducer2 extends MapReduceBase implements
			Reducer<IntWritable, IntWritable, IntWritable, Text> {

		private final static Text sText = new Text();

		public void reduce(IntWritable key, Iterator<IntWritable> values,
				OutputCollector<IntWritable, Text> output, Reporter reporter) throws IOException {

			StringBuilder sb = new StringBuilder();
			Set<Integer> set = new HashSet<Integer>();

			IntWritable cur;
			while (values.hasNext()) {
				cur = values.next();

				if (cur.get() == key.get())
					continue;

				// keep only one link to target
				if (set.contains(cur.get()))
					continue;

				set.add(cur.get());

				sb.append(cur.get());
				sb.append("\t");
			}

			sText.set(sb.toString());
			output.collect(key, sText);

		}
	}

	/**
	 * Creates an instance of this tool.
	 */
	public BuildWikipediaLinkGraph() {
	}

	private static int printUsage() {
		System.out
				.println("usage: [input] [edges-output] [adjacency-list-output] [num-partitions]");
		ToolRunner.printGenericCommandUsage(System.out);
		return -1;
	}

	/**
	 * Runs this tool.
	 */
	public int run(String[] args) throws Exception {
		if (args.length != 4) {
			printUsage();
			return -1;
		}

		int numPartitions = Integer.parseInt(args[3]);

		task1(args[0], args[1], numPartitions);
		task2(args[1], args[2], numPartitions);

		return 0;
	}

	private void task1(String inputPath, String outputPath, int partitions) throws IOException {
		sLogger.info("Exracting edges...");
		sLogger.info(" - input: " + inputPath);
		sLogger.info(" - output: " + outputPath);

		JobConf conf = new JobConf(BuildWikipediaLinkGraph.class);
		conf.setJobName("BuildWikipediaLinkGraph:Edges");

		conf.setNumMapTasks(10);
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

		// delete the output directory if it exists already
		FileSystem.get(conf).delete(new Path(outputPath), true);

		JobClient.runJob(conf);
	}

	private void task2(String inputPath, String outputPath, int partitions) throws IOException {
		sLogger.info("Building adjacency lists...");
		sLogger.info(" - input: " + inputPath);
		sLogger.info(" - output: " + outputPath);

		JobConf conf = new JobConf(BuildWikipediaLinkGraph.class);
		conf.setJobName("BuildWikipediaLinkGraph:AdjacencyList");

		conf.setNumMapTasks(10);
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

		// delete the output directory if it exists already
		FileSystem.get(conf).delete(new Path(outputPath), true);

		JobClient.runJob(conf);
	}

	/**
	 * Dispatches command-line arguments to the tool via the
	 * <code>ToolRunner</code>.
	 */
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new BuildWikipediaLinkGraph(), args);
		System.exit(res);
	}
}
