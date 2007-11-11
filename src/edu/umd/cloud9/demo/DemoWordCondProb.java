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
 * Demo that illustrates the use of a Partitioner and special symbols in Tuple
 * to compute conditional probabilities. Demo builds on
 * {@link DemoWordCountTuple}, and has similar structure. Input comes from
 * Bible+Shakespeare sample collection, encoded as single-field tuples; see
 * {@link DemoPackRecords}. Sample of final output:
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
 * (admired, 0)	0.6315789
 * (admired, 1)	0.36842105
 * ...
 * </pre>
 * 
 * <p>
 * The first field of the key tuple contains a token. If the second field
 * contains the special symbol '*', then the value indicates the count of the
 * token in the collection. Otherwise, the value indicates p(EvenOrOdd|token),
 * the probability that a line is odd-length or even-length, given the
 * occurrence of a token.
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
		private final static FloatWritable one = new FloatWritable(1);
		private Tuple tuple = KEY_SCHEMA.instantiate();

		public void map(WritableComparable key, Writable value,
				OutputCollector output, Reporter reporter) throws IOException {

			// the input value is a tuple; get field 0
			// see DemoPackRecords of how input SequenceFile is generated
			String line = (String) ((Tuple) value).get(0);
			StringTokenizer itr = new StringTokenizer(line);
			while (itr.hasMoreTokens()) {
				String token = itr.nextToken();

				// emit key-value pair for either even-length or odd-length line
				tuple.set("Token", token);
				tuple.set("EvenOrOdd", line.length() % 2);
				output.collect(tuple, one);

				// emit key-value pair for the total count
				tuple.set("Token", token);
				// use special symbol in field 2
				tuple.setSymbol("EvenOrOdd", "*");
				output.collect(tuple, one);
			}
		}
	}

	// reducer computes conditional probabilities
	private static class ReduceClass extends MapReduceBase implements Reducer {
		// HashMap keeps track of total counts
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

			// check if the second field is a special symbol
			if (((Tuple) key).containsSymbol("EvenOrOdd")) {
				// emit total count
				output.collect(key, new FloatWritable(sum));
				// record total count
				TotalCounts.put(tok, sum);
			} else {
				if (!TotalCounts.containsKey(tok))
					throw new UnexpectedException("Don't have total counts!");

				// divide sum by total count to obtain conditional probability
				float p = (float) sum / TotalCounts.get(tok);

				// emit P(EvenOrOdd|Token)
				output.collect(key, new FloatWritable(p));
			}
		}
	}

	// partition by first field of the tuple, so that tuples corresponding
	// to the same token will be sent to the same reducer
	private static class MyPartitioner implements Partitioner {
		public void configure(JobConf job) {
		}

		public int getPartition(WritableComparable key, Writable value,
				int numReduceTasks) {
			return (((Tuple) key).get("Token").hashCode() & Integer.MAX_VALUE)
					% numReduceTasks;
		}
	}

	// a Comparator that sorts the special symbol first
	private static class MyTupleComparator extends WritableComparator {
		public MyTupleComparator() {
			super(Tuple.class);
		}

		public int compare(WritableComparable a, WritableComparable b) {
			Tuple tA = (Tuple) a;
			Tuple tB = (Tuple) b;
			String tokenA = (String) tA.get("Token");
			String tokenB = (String) tB.get("Token");

			// if the tokens match, compare second field
			if (tokenA.equals(tokenB)) {
				// both contain symbol, so tuples must be equal
				if (tA.containsSymbol("EvenOrOdd")
						&& tB.containsSymbol("EvenOrOdd"))
					return 0;

				// tuple with special symbol always goes first
				if (tA.containsSymbol("EvenOrOdd"))
					return -1;

				// tuple with special symbol always goes first
				if (tB.containsSymbol("EvenOrOdd"))
					return 1;

				// otherwise, compare EvenOrOdd field
				return ((Integer) tA.get("EvenOrOdd")).compareTo((Integer) tB
						.get("EvenOrOdd"));
			}

			// compare the tokens
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
		String output1Path = "condprob-tuple";
		String output2Path = "condprob-txt";
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
		// this is a potential gotcha! can't use ReduceClass for combine because
		// we have not collected all the counts yet, so we can't divide through
		// to compute the conditional probabilities
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
