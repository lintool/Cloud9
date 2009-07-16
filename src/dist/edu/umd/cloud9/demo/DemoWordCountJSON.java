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
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.json.JSONException;

import edu.umd.cloud9.io.JSONObjectWritable;

/**
 * <p>
 * Demo that illustrates use of {@link JSONObjectWritable} objects as
 * intermediate keys in a MapReduce job. This Hadoop Tool takes the following
 * command-line arguments:
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
 * Input comes from a flat text collection packed into a SequenceFile with
 * {@link DemoPackJSON}. Output shows the count of words on even- and
 * odd-length lines.
 * </p>
 * 
 * <p>
 * Format of the output SequenceFile: The key is a JSON object. The field named
 * "Token" contains a word and the field named "EvenOrOdd" indicates whether the
 * word was found on a even-length or odd-length line. The value is the count of
 * the word on either even- or odd-length lines.
 * </p>
 * 
 * @see DemoWordCountTuple1
 * @see DemoWordCountTuple2
 * 
 * @author Jimmy Lin
 */
public class DemoWordCountJSON extends Configured implements Tool {
	private static final Logger sLogger = Logger.getLogger(DemoWordCountJSON.class);

	// define custom intermediate key; must specify sort order
	public static class MyKey extends JSONObjectWritable implements WritableComparable {
		public int compareTo(Object obj) {
			try {
				MyKey that = (MyKey) obj;

				String thisToken = this.getStringUnchecked("Token");
				String thatToken = that.getStringUnchecked("Token");

				// if tokens are equal, must check "EvenOrOdd" field
				if (thisToken.equals(thatToken)) {
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

		public int hashCode() {
			int val = -1;
			try {
				val = this.getStringUnchecked("Token").hashCode();
			} catch (JSONException e) {
				throw new RuntimeException("Unexpected error with JSON objects!");
			}
			return val;
		}
	}

	// mapper that emits a json object as the key, and value '1' for each
	// occurrence
	protected static class MyMapper extends MapReduceBase implements
			Mapper<LongWritable, JSONObjectWritable, MyKey, IntWritable> {

		// define value '1' statically so we can reuse the object, i.e., avoid
		// unnecessary object creation
		private final static IntWritable one = new IntWritable(1);

		// once again, reuse keys if possible
		private final static MyKey key = new MyKey();

		public void map(LongWritable dummy, JSONObjectWritable jsonIn,
				OutputCollector<MyKey, IntWritable> output, Reporter reporter) throws IOException {

			try {
				// the input value is a JSON object
				String line = (String) jsonIn.get("text");
				StringTokenizer itr = new StringTokenizer(line);
				while (itr.hasMoreTokens()) {
					String token = itr.nextToken();

					// put new values into the tuple
					key.clear();
					key.put("Token", token);
					key.put("EvenOrOdd", line.length() % 2);

					// emit key-value pair
					output.collect(key, one);
				}
			} catch (JSONException e) {
				e.printStackTrace();
				throw new RuntimeException("Unexpected error with JSON objects!");
			}
		}
	}

	// reducer counts up tuple occurrences
	protected static class MyReducer extends MapReduceBase implements
			Reducer<MyKey, IntWritable, MyKey, IntWritable> {
		private final static IntWritable SumValue = new IntWritable();

		public void reduce(MyKey keyIn, Iterator<IntWritable> values,
				OutputCollector<MyKey, IntWritable> output, Reporter reporter) throws IOException {
			// sum values
			int sum = 0;
			while (values.hasNext()) {
				sum += values.next().get();
			}

			// keep original tuple key, emit sum of counts as value
			SumValue.set(sum);
			output.collect(keyIn, SumValue);
		}
	}

	/**
	 * Creates an instance of this tool.
	 */
	public DemoWordCountJSON() {
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

		int numMapTasks = Integer.parseInt(args[2]);
		int numReduceTasks = Integer.parseInt(args[3]);

		sLogger.info("Tool: DemoWordCountJSON");
		sLogger.info(" - input path: " + inputPath);
		sLogger.info(" - output path: " + outputPath);
		sLogger.info(" - number of mappers: " + numMapTasks);
		sLogger.info(" - number of reducers: " + numReduceTasks);

		JobConf conf = new JobConf(DemoWordCountTuple1.class);
		conf.setJobName("DemoWordCountJSON");

		conf.setNumMapTasks(numMapTasks);
		conf.setNumReduceTasks(numReduceTasks);

		FileInputFormat.setInputPaths(conf, new Path(inputPath));
		FileOutputFormat.setOutputPath(conf, new Path(outputPath));
		FileOutputFormat.setCompressOutput(conf, false);

		conf.setInputFormat(SequenceFileInputFormat.class);
		conf.setOutputKeyClass(MyKey.class);
		conf.setOutputValueClass(IntWritable.class);
		conf.setOutputFormat(SequenceFileOutputFormat.class);

		conf.setMapperClass(MyMapper.class);
		conf.setCombinerClass(MyReducer.class);
		conf.setReducerClass(MyReducer.class);

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
		int res = ToolRunner.run(new Configuration(), new DemoWordCountJSON(), args);
		System.exit(res);
	}
}
