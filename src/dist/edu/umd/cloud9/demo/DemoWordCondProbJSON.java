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
import org.apache.hadoop.io.WritableComparable;
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
import org.json.JSONException;
import org.json.JSONObject;

import edu.umd.cloud9.io.JSONObjectWritable;

/**
 * <p>
 * Demo of how to compute conditional probabilities using JSON objects as
 * intermediate keys. See also {@link DemoWordCondProbTuple}. This Hadoop Tool
 * takes the following command-line arguments:
 * </p>
 * 
 * <ul>
 * <li>[input-path] input path</li>
 * <li>[output-path] output path</li>
 * <li>[num-mappers] number of mappers</li>
 * <li>[num-reducers] number of reducers</li>
 * </ul>
 * 
 * @author Jimmy Lin
 */
public class DemoWordCondProbJSON extends Configured implements Tool {
	private static final Logger sLogger = Logger.getLogger(DemoWordCondProbJSON.class);

	// define custom intermediate key; must specify sort order
	private static class MyTuple extends JSONObjectWritable implements WritableComparable {
		public int compareTo(Object obj) {
			try {
				MyTuple that = (MyTuple) obj;

				String thisToken = this.getStringUnchecked("Token");
				String thatToken = that.getStringUnchecked("Token");

				// if tokens are equal, must check "EvenOrOdd" field
				if (thisToken.equals(thatToken)) {
					// if both fields are null, then tuples are equal
					if (this.isNull("EvenOrOdd") && that.isNull("EvenOrOdd"))
						return 0;

					// null field should always come first
					if (this.isNull("EvenOrOdd"))
						return -1;

					if (that.isNull("EvenOrOdd"))
						return 1;

					// otherwise, sort by "EvenOrOdd" field
					int thisEO = this.getIntUnchecked("EvenOrOdd");
					int thatEO = that.getIntUnchecked("EvenOrOdd");

					if (thisEO < thatEO)
						return -1;

					if (thisEO > thatEO)
						return 1;

					// if we get here, it means the tuples are equal
					return 0;
				}

				// determine sort order based on token
				return thisToken.compareTo(thatToken);
			} catch (JSONException e) {
				e.printStackTrace();
				throw new RuntimeException("Unexpected error comparing JSON objects!");
			}
		}
	}

	// mapper that emits tuple as the key, and value '1' for each occurrence
	protected static class MyMapper extends MapReduceBase implements
			Mapper<LongWritable, Text, MyTuple, FloatWritable> {
		private FloatWritable one = new FloatWritable(1);
		private MyTuple tuple = new MyTuple();

		public void map(LongWritable key, Text text,
				OutputCollector<MyTuple, FloatWritable> output, Reporter reporter)
				throws IOException {

			String line = (String) new String(text.toString());
			StringTokenizer itr = new StringTokenizer(line);
			while (itr.hasMoreTokens()) {
				String token = itr.nextToken();

				// emit key-value pair for either even-length or odd-length line
				try {
					tuple.put("Token", token);
					tuple.put("EvenOrOdd", line.length() % 2);
					output.collect(tuple, one);
				} catch (JSONException e) {
					e.printStackTrace();
					throw new RuntimeException("Unexpected error manipulating JSON object!");
				}

				// emit key-value pair for the total count
				try {
					tuple.put("Token", token);
					tuple.put("EvenOrOdd", JSONObject.NULL);
					output.collect(tuple, one);
				} catch (JSONException e) {
					e.printStackTrace();
					throw new RuntimeException("Unexpected error manipulating JSON object!");
				}
			}
		}
	}

	// reducer computes conditional probabilities
	protected static class MyReducer extends MapReduceBase implements
			Reducer<MyTuple, FloatWritable, MyTuple, FloatWritable> {
		// HashMap keeps track of total counts
		private HashMap<String, Integer> TotalCounts = new HashMap<String, Integer>();

		public void reduce(MyTuple tupleKey, Iterator<FloatWritable> values,
				OutputCollector<MyTuple, FloatWritable> output, Reporter reporter)
				throws IOException {

			// sum values
			int sum = 0;
			while (values.hasNext()) {
				sum += values.next().get();
			}

			try {
				String tok = (String) tupleKey.getString("Token");

				// check if the "EvenOrOdd" field is a special symbol
				if (tupleKey.isNull("EvenOrOdd")) {
					// emit total count
					output.collect(tupleKey, new FloatWritable(sum));
					// record total count
					TotalCounts.put(tok, sum);
				} else {
					if (!TotalCounts.containsKey(tok))
						throw new UnexpectedException("Don't have total counts!");

					// divide sum by total count to obtain conditional
					// probability
					float p = (float) sum / TotalCounts.get(tok);

					// emit P(EvenOrOdd|Token)
					output.collect(tupleKey, new FloatWritable(p));
				}
			} catch (JSONException e) {
				e.printStackTrace();
				throw new RuntimeException("Unexpected error manipulating JSON object!");
			}
		}
	}

	// partition by token, so that tuples corresponding to the same token will
	// be sent to the same reducer
	protected static class MyPartitioner implements Partitioner<MyTuple, FloatWritable> {
		public void configure(JobConf job) {
		}

		public int getPartition(MyTuple key, FloatWritable value, int numReduceTasks) {
			int hash = -1;

			try {
				hash = (key.getString("Token").hashCode() & Integer.MAX_VALUE) % numReduceTasks;
			} catch (JSONException e) {
				e.printStackTrace();
				throw new RuntimeException("Unexpected error manipulating JSON object!");
			}

			return hash;
		}
	}

	/**
	 * Creates an instance of this tool.
	 */
	public DemoWordCondProbJSON() {
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

		sLogger.info("Tool: DemoWordCondProbJSON");
		sLogger.info(" - input path: " + inputPath);
		sLogger.info(" - output path: " + outputPath);
		sLogger.info(" - number of mappers: " + mapTasks);
		sLogger.info(" - number of reducers: " + reduceTasks);

		JobConf conf = new JobConf(DemoWordCondProbJSON.class);
		conf.setJobName("DemoWordCondProbJSON");

		conf.setNumMapTasks(mapTasks);
		conf.setNumReduceTasks(reduceTasks);

		FileInputFormat.setInputPaths(conf, new Path(inputPath));
		FileOutputFormat.setOutputPath(conf, new Path(outputPath));
		FileOutputFormat.setCompressOutput(conf, false);

		conf.setOutputKeyClass(MyTuple.class);
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
		int res = ToolRunner.run(new Configuration(), new DemoWordCondProbJSON(), args);
		System.exit(res);
	}
}
