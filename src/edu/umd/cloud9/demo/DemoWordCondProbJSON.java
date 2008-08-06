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
import org.json.JSONException;
import org.json.JSONObject;

import edu.umd.cloud9.io.JSONObjectWritable;

/**
 * <p>
 * Computes the same thing as {@link DemoWordCondProbTuple}, except uses JSON
 * objects as intermediate keys. Expected output:
 * </p>
 * 
 * <pre>
 * Map input records=156215
 * Map output records=3468596
 * Map input bytes=9068074
 * Map output bytes=123756588
 * Combine input records=3468596
 * Combine output records=324085
 * Reduce input groups=101013
 * Reduce input records=3468596
 * Reduce output records=101013
 * </pre>
 */
public class DemoWordCondProbJSON {

	// must be public
	public static class MyTuple extends JSONObjectWritable implements WritableComparable {
		public MyTuple() {
			super();
		}

		public MyTuple(String s) throws JSONException {
			super(s);
		}

		public int compareTo(Object obj) {
			try {
				MyTuple that = (MyTuple) obj;

				String thisToken = this.getString("Token");
				String thatToken = that.getString("Token");

				if (thisToken.equals(thatToken)) {
					if (this.isNull("EvenOrOdd") && that.isNull("EvenOrOdd"))
						return 0;

					if (this.isNull("EvenOrOdd"))
						return -1;

					if (that.isNull("EvenOrOdd"))
						return 1;

					int thisEO = this.getIntUnchecked("EvenOrOdd");
					int thatEO = that.getIntUnchecked("EvenOrOdd");

					if (thisEO < thatEO)
						return -1;

					if (thisEO > thatEO)
						return 1;

					return 0;
				}

				return thisToken.compareTo(thatToken);
			} catch (JSONException e) {
				e.printStackTrace();
				throw new RuntimeException("Unexpected error comparing JSON objects!");
			}
		}

		public MyTuple clone() {
			MyTuple j = null;
			try {
				j = new MyTuple(this.toString());
			} catch (JSONException e) {
				e.printStackTrace();
			}

			return j;
		}
	}

	// mapper that emits tuple as the key, and value '1' for each occurrence
	private static class MyMapper extends MapReduceBase implements
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
	private static class MyReducer extends MapReduceBase implements
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
	private static class MyPartitioner implements Partitioner<MyTuple, FloatWritable> {
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

	// dummy constructor
	private DemoWordCondProbJSON() {
	}

	/**
	 * Runs the demo.
	 */
	public static void main(String[] args) throws IOException {
		String inPath = "/shared/sample-input/bible+shakes.nopunc";
		String outputPath = "condprob";
		int numMapTasks = 20;
		int numReduceTasks = 10;

		JobConf conf = new JobConf(DemoWordCondProbJSON.class);
		conf.setJobName("DemoWordCondProbJSON");

		conf.setNumMapTasks(numMapTasks);
		conf.setNumReduceTasks(numReduceTasks);

		FileInputFormat.setInputPaths(conf, new Path(inPath));
		FileOutputFormat.setOutputPath(conf, new Path(outputPath));

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

		JobClient.runJob(conf);
	}
}
