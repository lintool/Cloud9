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

package edu.umd.cloud9.bigram;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Partitioner;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import edu.umd.cloud9.io.PairOfStrings;

public class BigramRelativeFrequency extends Configured implements Tool {

	private static final Logger sLogger = Logger.getLogger(BigramRelativeFrequency.class);

	// mapper: emits (token, 1) for every bigram occurrence
	protected static class MyMapper extends MapReduceBase implements
			Mapper<LongWritable, Text, PairOfStrings, FloatWritable> {

		// reuse objects to save overhead of object creation
		private final static FloatWritable one = new FloatWritable(1);
		private PairOfStrings bigrams = new PairOfStrings();

		public void map(LongWritable key, Text value,
				OutputCollector<PairOfStrings, FloatWritable> output, Reporter reporter)
				throws IOException {
			String line = ((Text) value).toString();

			String prev = null;
			StringTokenizer itr = new StringTokenizer(line);
			while (itr.hasMoreTokens()) {
				String cur = itr.nextToken();

				// emit only if we have an actual bigram
				if (prev != null) {

					// simple way to truncate tokens that are too long
					if (cur.length() > 100)
						cur = cur.substring(0, 100);

					if (prev.length() > 100)
						prev = prev.substring(0, 100);

					bigrams.set(prev, cur);
					output.collect(bigrams, one);

					bigrams.set(prev, "*");
					output.collect(bigrams, one);
				}
				prev = cur;
			}
		}
	}

	// combiner: sums up all the counts
	protected static class MyCombiner extends MapReduceBase implements
			Reducer<PairOfStrings, FloatWritable, PairOfStrings, FloatWritable> {

		// reuse objects
		private final static FloatWritable SumValue = new FloatWritable();

		public void reduce(PairOfStrings key, Iterator<FloatWritable> values,
				OutputCollector<PairOfStrings, FloatWritable> output, Reporter reporter)
				throws IOException {
			// sum up values
			int sum = 0;
			while (values.hasNext()) {
				sum += values.next().get();
			}
			SumValue.set(sum);
			output.collect(key, SumValue);
		}
	}

	// reducer: sums up all the counts
	protected static class MyReducer extends MapReduceBase implements
			Reducer<PairOfStrings, FloatWritable, PairOfStrings, FloatWritable> {

		// reuse objects
		private final static FloatWritable value = new FloatWritable();

		private float marginal = 0.0f;

		public void reduce(PairOfStrings key, Iterator<FloatWritable> values,
				OutputCollector<PairOfStrings, FloatWritable> output, Reporter reporter)
				throws IOException {
			// sum up values
			float sum = 0.0f;
			while (values.hasNext()) {
				sum += values.next().get();
			}

			if (key.getRightElement().equals("*")) {
				value.set(sum);
				output.collect(key, value);
				marginal = sum;
			} else {
				value.set(sum / marginal);
				output.collect(key, value);
			}
		}
	}

	protected static class MyPartitioner implements Partitioner<PairOfStrings, FloatWritable> {
		public void configure(JobConf job) {
		}

		public int getPartition(PairOfStrings key, FloatWritable value, int numReduceTasks) {
			return (key.getLeftElement().hashCode() & Integer.MAX_VALUE) % numReduceTasks;
		}
	}

	private BigramRelativeFrequency() {
	}

	private static int printUsage() {
		System.out.println("usage: [input-path] [output-path] [num-mappers] [num-reducers]");
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

		String inputPath = args[0];
		String outputPath = args[1];
		int mapTasks = Integer.parseInt(args[2]);
		int reduceTasks = Integer.parseInt(args[3]);

		sLogger.info("Tool name: BigramRelativeFrequency");
		sLogger.info(" - input path: " + inputPath);
		sLogger.info(" - output path: " + outputPath);
		sLogger.info(" - num mappers: " + mapTasks);
		sLogger.info(" - num reducers: " + reduceTasks);

		JobConf conf = new JobConf(BigramRelativeFrequency.class);
		conf.setJobName("BigramRelativeFrequency");

		conf.setNumMapTasks(mapTasks);
		conf.setNumReduceTasks(reduceTasks);

		FileInputFormat.setInputPaths(conf, new Path(inputPath));
		FileOutputFormat.setOutputPath(conf, new Path(outputPath));

		conf.setOutputKeyClass(PairOfStrings.class);
		conf.setOutputValueClass(FloatWritable.class);
		conf.setOutputFormat(SequenceFileOutputFormat.class);

		conf.setMapperClass(MyMapper.class);
		conf.setCombinerClass(MyCombiner.class);
		conf.setReducerClass(MyReducer.class);
		conf.setPartitionerClass(MyPartitioner.class);

		// Delete the output directory if it exists already
		Path outputDir = new Path(outputPath);
		FileSystem.get(conf).delete(outputDir, true);

		JobClient.runJob(conf);

		return 0;
	}

	/**
	 * Dispatches command-line arguments to the tool via the
	 * <code>ToolRunner</code>.
	 */
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new BigramRelativeFrequency(), args);
		System.exit(res);
	}
}
