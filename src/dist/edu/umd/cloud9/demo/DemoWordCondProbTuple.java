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

package edu.umd.cloud9.demo;

import java.io.IOException;
import java.rmi.UnexpectedException;
import java.util.HashMap;
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
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.lib.IdentityReducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import edu.umd.cloud9.io.Schema;
import edu.umd.cloud9.io.Tuple;

/**
 * <p>
 * Demo of how to compute conditional probabilities using Tuples as intermediate
 * keys. See also {@link DemoWordCondProbJSON}. This Hadoop Tool takes the
 * following command-line arguments:
 * </p>
 * 
 * <ul>
 * <li>[input-path] input path</li>
 * <li>[output-path] output path</li>
 * <li>[num-mappers] number of mappers</li>
 * <li>[num-reducers] number of reducers</li>
 * </ul>
 * 
 * <p>
 * Sample of final output on the Bible+Shakespeare sample collection:
 * 
 * <pre>
 * ...
 * (admirable, *)   15.0
 * (admirable, 0)   0.6
 * (admirable, 1)   0.4
 * (admiral, *)     6.0
 * (admiral, 0)     0.33333334
 * (admiral, 1)     0.6666667
 * (admiration, *)  16.0
 * (admiration, 0)  0.625
 * (admiration, 1)  0.375
 * (admire, *)      8.0
 * (admire, 0)      0.625
 * (admire, 1)      0.375
 * (admired, *)     19.0
 * (admired, 0)     0.6315789
 * (admired, 1)     0.36842105
 * ...
 * </pre>
 * 
 * <p>
 * The first field of the key tuple contains a token. If the second field
 * contains the special symbol '*', then the value indicates the count of the
 * token in the collection. Otherwise, the value indicates p(EvenOrOdd|Token),
 * the probability that a line is odd-length or even-length, given the
 * occurrence of a token.
 * </p>
 * 
 * @author Jimmy Lin
 */
public class DemoWordCondProbTuple extends Configured implements Tool {
	private static final Logger sLogger = Logger.getLogger(DemoWordCondProbTuple.class);

	// create the schema for the tuple that will serve as the key
	private static final Schema KEY_SCHEMA = new Schema();

	// define the schema statically
	static {
		KEY_SCHEMA.addField("Token", String.class, "");
		KEY_SCHEMA.addField("EvenOrOdd", Integer.class, new Integer(1));
	}

	// mapper that emits tuple as the key, and value '1' for each occurrence
	protected static class MyMapper extends MapReduceBase implements
			Mapper<LongWritable, Text, Tuple, FloatWritable> {
		private final static FloatWritable one = new FloatWritable(1);
		private Tuple tupleOut = KEY_SCHEMA.instantiate();

		public void map(LongWritable key, Text text, OutputCollector<Tuple, FloatWritable> output,
				Reporter reporter) throws IOException {
			String line = text.toString();
			StringTokenizer itr = new StringTokenizer(line);
			while (itr.hasMoreTokens()) {
				String token = itr.nextToken();

				// emit key-value pair for either even-length or odd-length line
				tupleOut.set("Token", token);
				tupleOut.set("EvenOrOdd", line.length() % 2);
				output.collect(tupleOut, one);

				// emit key-value pair for the total count
				tupleOut.set("Token", token);
				// use special symbol in field 2
				tupleOut.setSymbol("EvenOrOdd", "*");
				output.collect(tupleOut, one);
			}
		}
	}

	// reducer computes conditional probabilities
	protected static class MyReducer extends MapReduceBase implements
			Reducer<Tuple, FloatWritable, Tuple, FloatWritable> {
		// HashMap keeps track of total counts
		private final static HashMap<String, Integer> TotalCounts = new HashMap<String, Integer>();

		public synchronized void reduce(Tuple tupleKey, Iterator<FloatWritable> values,
				OutputCollector<Tuple, FloatWritable> output, Reporter reporter) throws IOException {
			// sum values
			int sum = 0;
			while (values.hasNext()) {
				sum += values.next().get();
			}

			String tok = (String) tupleKey.get("Token");

			// check if the second field is a special symbol
			if (tupleKey.containsSymbol("EvenOrOdd")) {
				// emit total count
				output.collect(tupleKey, new FloatWritable(sum));
				// record total count
				TotalCounts.put(tok, sum);
			} else {
				if (!TotalCounts.containsKey(tok))
					throw new UnexpectedException("Don't have total counts!");

				// divide sum by total count to obtain conditional probability
				float p = (float) sum / TotalCounts.get(tok);

				// emit P(EvenOrOdd|Token)
				output.collect(tupleKey, new FloatWritable(p));
			}
		}
	}

	// partition by first field of the tuple, so that tuples corresponding
	// to the same token will be sent to the same reducer
	protected static class MyPartitioner implements Partitioner<Tuple, FloatWritable> {
		public void configure(JobConf job) {
		}

		public int getPartition(Tuple key, FloatWritable value, int numReduceTasks) {
			return (key.get("Token").hashCode() & Integer.MAX_VALUE) % numReduceTasks;
		}
	}

	/**
	 * Creates an instance of this tool.
	 */
	public DemoWordCondProbTuple() {
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

		sLogger.info("Tool: DemoWordCondProbTuple");
		sLogger.info(" - input path: " + inputPath);
		sLogger.info(" - output path: " + outputPath);
		sLogger.info(" - number of mappers: " + mapTasks);
		sLogger.info(" - number of reducers: " + reduceTasks);

		JobConf conf = new JobConf(DemoWordCondProbTuple.class);
		conf.setJobName("DemoWordCondProbTuple");

		conf.setNumMapTasks(mapTasks);
		conf.setNumReduceTasks(reduceTasks);

		FileInputFormat.setInputPaths(conf, new Path(inputPath));
		FileOutputFormat.setOutputPath(conf, new Path(outputPath));
		FileOutputFormat.setCompressOutput(conf, false);

		conf.setOutputKeyClass(Tuple.class);
		conf.setOutputValueClass(FloatWritable.class);
		conf.setOutputFormat(TextOutputFormat.class);

		conf.setMapperClass(MyMapper.class);
		// this is a potential gotcha! can't use ReduceClass for combine because
		// we have not collected all the counts yet, so we can't divide through
		// to compute the conditional probabilities
		conf.setCombinerClass(IdentityReducer.class);
		conf.setReducerClass(MyReducer.class);
		conf.setPartitionerClass(MyPartitioner.class);

		// Delete the output directory if it exists already
		Path outputDir = new Path(outputPath);
		FileSystem.get(conf).delete(outputDir, true);

		long startTime = System.currentTimeMillis();
		JobClient.runJob(conf);
		sLogger.info("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0
				+ " seconds");

		return 0;
	}

	/**
	 * Dispatches command-line arguments to the tool via the
	 * <code>ToolRunner</code>.
	 */
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new DemoWordCondProbTuple(), args);
		System.exit(res);
	}

}
