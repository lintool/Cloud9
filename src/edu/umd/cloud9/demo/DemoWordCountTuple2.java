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

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
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

import edu.umd.cloud9.tuple.ListWritable;
import edu.umd.cloud9.tuple.Schema;
import edu.umd.cloud9.tuple.Tuple;

/**
 * <p>
 * Demo that illustrates the use of the tuple library. Input comes from
 * Bible+Shakespeare sample collection, encoded as single-field tuples; see
 * {@link DemoPackRecords}. Sample of final output:
 * </p>
 * 
 * <pre>
 * ...
 * (admirable, 0)    9
 * (admirable, 1)    6
 * (admiral, 0)      2
 * (admiral, 1)      4
 * (admiration, 0)  10
 * (admiration, 1)   6
 * (admire, 0)       5
 * (admire, 1)       3
 * (admired, 0)     12
 * (admired, 1)      7
 * ...
 * </pre>
 * 
 * <p>
 * The first field of the key tuple contains a token, the second field indicates
 * whether it was found on a even-length or odd-length line. The value is the
 * count of the tuple occurrences in the collection. The demo executes two
 * separate MapReduce cycles:
 * </p>
 * 
 * <ul>
 * <li> In the first MapReduce cycle, output keys consist of tuples (Token,
 * EvenOrOdd). The second field of the tuple indicates whether the token was
 * found on a line with an even or an odd number of characters. Values consist
 * of counts of tuple occurrences. </li>
 * <li> In the second MapReduce cycle, the tuple keys are decoded back into a
 * text representation. </li>
 * </ul>
 * <p>
 * Obviously, this isn't a particularly meaningful program, but does illustrate
 * the use of the {@link Tuple} class.
 * </p>
 */
public class DemoWordCountTuple2 {

	// create the schema for the tuple that will serve as the key
	private static final Schema KEY_SCHEMA = new Schema();

	// define the schema statically
	static {
		KEY_SCHEMA.addField("Token", String.class, "");
		KEY_SCHEMA.addField("EvenOrOdd", Integer.class, new Integer(1));
	}

	// mapper that emits tuple as the key, and value '1' for each occurrence
	private static class MapClass extends MapReduceBase implements
			Mapper<LongWritable, Tuple, Tuple, IntWritable> {

		// define value '1' statically so we can reuse the object, i.e., avoid
		// unnecessary object creation
		private final static IntWritable one = new IntWritable(1);

		// once again, reuse tuples if possible
		private Tuple tupleOut = KEY_SCHEMA.instantiate();

		public void map(LongWritable key, Tuple tupleIn,
				OutputCollector<Tuple, IntWritable> output, Reporter reporter)
				throws IOException {

			@SuppressWarnings("unchecked")
			ListWritable<Text> list = (ListWritable<Text>) tupleIn.get(1);
			
			for ( int i=0; i<list.size(); i++) {
				Text t = (Text) list.get(i);
			
				String token = t.toString();

				// put new values into the tuple
				tupleOut.set("Token", token);
				tupleOut.set("EvenOrOdd", ((Integer) tupleIn.get(0)) % 2);

				// emit key-value pair
				output.collect(tupleOut, one);
			}
		}
	}

	// reducer counts up tuple occurrences
	private static class ReduceClass extends MapReduceBase implements
			Reducer<Tuple, IntWritable, Tuple, IntWritable> {
		private final static IntWritable SumValue = new IntWritable();

		public synchronized void reduce(Tuple tupleKey,
				Iterator<IntWritable> values,
				OutputCollector<Tuple, IntWritable> output, Reporter reporter)
				throws IOException {
			// sum values
			int sum = 0;
			while (values.hasNext()) {
				sum += values.next().get();
			}

			// keep original tuple key, emit sum of counts as value
			SumValue.set(sum);
			output.collect(tupleKey, SumValue);
		}
	}

	// mapper that unpacks the serialized tuples back into human-readable text
	private static class UnpackKeysClass extends MapReduceBase implements
			Mapper<Tuple, IntWritable, Text, IntWritable> {
		private Text text = new Text();

		public void map(Tuple tupleIn, IntWritable sum,
				OutputCollector<Text, IntWritable> output, Reporter reporter)
				throws IOException {
			text.set(tupleIn.toString());
			output.collect(text, sum);
		}
	}

	// dummy constructor
	private DemoWordCountTuple2() {
	}

	/**
	 * Runs the demo.
	 */
	public static void main(String[] args) throws IOException {
		String inPath = "/shared/sample-input/bible+shakes.nopunc.packed2";
		String output1Path = "word-counts2-tuple";
		String output2Path = "word-counts2-txt";
		int numMapTasks = 20;
		int numReduceTasks = 20;

		// first MapReduce cycle is to do the tuple counting
		JobConf conf1 = new JobConf(DemoWordCountTuple2.class);
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
		JobConf conf2 = new JobConf(DemoWordCountTuple2.class);
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
