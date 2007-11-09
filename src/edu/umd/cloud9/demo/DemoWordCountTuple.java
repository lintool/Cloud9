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

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.lib.IdentityReducer;

import edu.umd.cloud9.tuple.Schema;
import edu.umd.cloud9.tuple.Tuple;

/**
 * <p>
 * Demo that illustrates the use of the tuple library. Input comes from
 * bible+Shakespeare sample collection, encoded as single-field tuples; see
 * {@link DemoPackRecords}. The demo executes two separate MapReduce cycles:
 * </p>
 * 
 * <ul>
 * 
 * <li>In the first MapReduce cycle, output keys consist of tuples (Token,
 * EvenOrOdd). The second field of the tuple indicates whether the token was
 * found on a line with an even or an odd number of characters. Values consist
 * of counts of tuple occurrences.</li>
 * 
 * <li>In the second MapReduce cycle, the tuple keys are decoded back into a
 * text representation.</li>
 * 
 * </ul>
 * 
 * <p>
 * Obviously, this isn't a particularly meaningful program, but does illustrate
 * the use of the {@link Tuple} class.
 * </p>
 */
public class DemoWordCountTuple {

	// create the schema for the tuple that will serve as the key
	private static final Schema KEY_SCHEMA = new Schema();

	// define the schema statically
	static {
		KEY_SCHEMA.addField("Token", String.class, "");
		KEY_SCHEMA.addField("EvenOrOdd", Integer.class, new Integer(1));
	}

	// mapper that emits tuple as the key, and value '1' for each occurrence
	private static class MapClass extends MapReduceBase implements Mapper {

		// define value '1' statically so we can reuse the object, i.e., avoid
		// unnecessary object creation
		private final static IntWritable one = new IntWritable(1);

		// once again, reuse tuples if possible
		private Tuple tuple = KEY_SCHEMA.instantiate();

		public void map(WritableComparable key, Writable value,
				OutputCollector output, Reporter reporter) throws IOException {

			// the input value is a tuple; get field 0
			// see DemoPackRecords of how input SequenceFile is generated
			String line = (String) ((Tuple) value).get(0);
			StringTokenizer itr = new StringTokenizer(line);
			while (itr.hasMoreTokens()) {
				String token = itr.nextToken();

				// put new values into the tuple
				tuple.set("Token", token);
				tuple.set("EvenOrOdd", line.length() % 2);

				// emit key-value pair
				output.collect(tuple, one);
			}
		}
	}

	// reducer counts up tuple occurrences
	private static class ReduceClass extends MapReduceBase implements Reducer {
		private final static IntWritable sumValue = new IntWritable();

		public synchronized void reduce(WritableComparable key,
				Iterator values, OutputCollector output, Reporter reporter)
				throws IOException {
			// sum values
			int sum = 0;
			while (values.hasNext()) {
				sum += ((IntWritable) values.next()).get();
			}

			// keep original tuple key, emit sum of counts as value
			sumValue.set(sum);
			output.collect(key, sumValue);
		}
	}

	// mapper that unpacks the serialized tuples back into human-readable text
	private static class UnpackKeysClass extends MapReduceBase implements
			Mapper {
		private Text textkey = new Text();

		public void map(WritableComparable key, Writable value,
				OutputCollector output, Reporter reporter) throws IOException {
			textkey.set(key.toString());
			output.collect(textkey, value);
		}
	}

	// dummy constructor
	private DemoWordCountTuple() {
	}

	/**
	 * Runs the demo.
	 */
	public static void main(String[] args) throws IOException {
		String inPath = "sample-input/bible+shakes.nopunc.packed";
		String output1Path = "word-counts-tuple";
		String output2Path = "word-counts-txt";
		int numMapTasks = 20;
		int numReduceTasks = 20;

		// first MapReduce cycle is to do the tuple counting
		JobConf conf1 = new JobConf(DemoWordCountTuple.class);
		conf1.setJobName("wordcount");

		conf1.setNumMapTasks(numMapTasks);
		conf1.setNumReduceTasks(numReduceTasks);

		conf1.setInputPath(new Path(inPath));
		conf1.setInputFormat(SequenceFileInputFormat.class);

		conf1.setOutputPath(new Path(output1Path));
		conf1.setOutputKeyClass(Tuple.class);
		conf1.setOutputValueClass(IntWritable.class);
		conf1.setOutputFormat(SequenceFileOutputFormat.class);

		conf1.setMapperClass(MapClass.class);
		conf1.setCombinerClass(ReduceClass.class);
		conf1.setReducerClass(ReduceClass.class);

		JobClient.runJob(conf1);

		// second MapReduce cycle is to unpack serialized tuples back into
		// human-readable text
		JobConf conf2 = new JobConf(DemoWordCountTuple.class);
		conf2.setJobName("unpack");

		conf2.setNumMapTasks(numMapTasks);
		conf2.setNumReduceTasks(1);

		conf2.setInputPath(new Path(output1Path));
		conf2.setInputFormat(SequenceFileInputFormat.class);

		conf2.setOutputPath(new Path(output2Path));
		conf2.setOutputKeyClass(Text.class);
		conf2.setOutputValueClass(IntWritable.class);

		conf2.setMapperClass(UnpackKeysClass.class);
		conf2.setCombinerClass(IdentityReducer.class);
		conf2.setReducerClass(IdentityReducer.class);

		JobClient.runJob(conf2);
	}
}
