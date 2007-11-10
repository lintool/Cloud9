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

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Partitioner;
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
public class DemoWordCondProb {

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
		private final static FloatWritable one = new FloatWritable(1);

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

				// put new values into the tuple
				tuple.set("Token", token);
				tuple.set("EvenOrOdd", -1);
				output.collect(tuple, one);
			}
		}
	}

	// reducer counts up tuple occurrences
	private static class ReduceClass extends MapReduceBase implements Reducer {
		private final static FloatWritable CondProb = new FloatWritable();
		private final static HashMap<String, Integer> TotalCounts = new HashMap<String, Integer>();

		public synchronized void reduce(WritableComparable key,
				Iterator values, OutputCollector output, Reporter reporter)
				throws IOException {
			// sum values
			int sum = 0;
			while (values.hasNext()) {
				sum += ((FloatWritable) values.next()).get();
			}

			String tok = (String) ((Tuple) key).get("Token");
			int EvenOrOdd = (Integer) ((Tuple) key).get("EvenOrOdd");

			if (EvenOrOdd == -1) {
				output.collect(key, new FloatWritable(sum));
				TotalCounts.put(tok, sum);
			} else {
				if (!TotalCounts.containsKey(tok))
					throw new UnexpectedException("Don't have total counts!");

				CondProb.set((float) sum / (float) TotalCounts.get(tok));
				//output.collect(key, CondProb);
				float p = (float) sum / TotalCounts.get(tok);
				
				//if ( TotalCounts.containsKey(tok))
					//throw new UnexpectedException("Token=" + tok + ", p=" + p);
				
				output.collect(key, new FloatWritable(p));
			}
		}
	}

	/** Partition keys by their {@link Object#hashCode()}. */
	private static class MyPartitioner implements Partitioner {

		public void configure(JobConf job) {
		}

		/** Use {@link Object#hashCode()} to partition. */
		public int getPartition(WritableComparable key, Writable value,
				int numReduceTasks) {
			return (((Tuple) key).get("Token").hashCode() & Integer.MAX_VALUE)
					% numReduceTasks;

		}

	}

	/** A Comparator optimized for Tuple. */
	private static class MyTupleComparator extends WritableComparator {
		public MyTupleComparator() {
			super(Tuple.class);
		}

		public int compare(WritableComparable a, WritableComparable b) {
			String tokenA = (String) ((Tuple) a).get("Token");
			String tokenB = (String) ((Tuple) b).get("Token");

			if (tokenA.equals(tokenB)) {
				return ((Integer) ((Tuple) a).get("EvenOrOdd"))
						.compareTo((Integer) ((Tuple) b).get("EvenOrOdd"));
			}

			return tokenA.compareTo(tokenB);
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
	private DemoWordCondProb() {
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
		JobConf conf1 = new JobConf(DemoWordCondProb.class);
		conf1.setJobName("DemoWordCondProb.MR1");

		conf1.setNumMapTasks(numMapTasks);
		conf1.setNumReduceTasks(numReduceTasks);

		conf1.setInputPath(new Path(inPath));
		conf1.setInputFormat(SequenceFileInputFormat.class);

		conf1.setOutputPath(new Path(output1Path));
		conf1.setOutputKeyClass(Tuple.class);
		conf1.setOutputValueClass(FloatWritable.class);
		conf1.setOutputFormat(SequenceFileOutputFormat.class);

		conf1.setMapperClass(MapClass.class);
		conf1.setCombinerClass(IdentityReducer.class);
		conf1.setReducerClass(ReduceClass.class);
		conf1.setPartitionerClass(MyPartitioner.class);
		conf1.setOutputKeyComparatorClass(MyTupleComparator.class);

		JobClient.runJob(conf1);

		// second MapReduce cycle is to unpack serialized tuples back into
		// human-readable text
		JobConf conf2 = new JobConf(DemoWordCondProb.class);
		conf2.setJobName("DemoWordCondProb.MR2");

		conf2.setNumMapTasks(numMapTasks);
		conf2.setNumReduceTasks(1);

		conf2.setInputPath(new Path(output1Path));
		conf2.setInputFormat(SequenceFileInputFormat.class);

		conf2.setOutputPath(new Path(output2Path));
		conf2.setOutputKeyClass(Text.class);
		conf2.setOutputValueClass(FloatWritable.class);

		conf2.setMapperClass(UnpackKeysClass.class);
		conf2.setCombinerClass(IdentityReducer.class);
		conf2.setReducerClass(IdentityReducer.class);

		JobClient.runJob(conf2);
	}
}
