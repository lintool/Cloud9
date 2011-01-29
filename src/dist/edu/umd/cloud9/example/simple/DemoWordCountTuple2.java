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

package edu.umd.cloud9.example.simple;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import edu.umd.cloud9.io.Schema;
import edu.umd.cloud9.io.Tuple;
import edu.umd.cloud9.io.array.ArrayListWritable;

/**
 * <p>
 * Another demo that illustrates use of {@link Tuple} objects as intermediate
 * keys in a MapReduce job. This Hadoop Tool takes the following command-line
 * arguments:
 * </p>
 * 
 * <ul>
 * <li>[input-path] input path</li>
 * <li>[output-path] output path</li>
 * <li>[num-reducers] number of reducers</li>
 * </ul>
 * 
 * <p>
 * Input comes from a flat text collection packed into a SequenceFile with
 * {@link DemoPackTuples2}; the tuples have complex internal structure. Output
 * shows the count of words on even- and odd-length lines. This demo does
 * exactly the same thing as {@link DemoWordCountTuple1}.
 * </p>
 * 
 * @see DemoWordCountTuple1
 * @see DemoWordCountJSON
 * 
 * @author Jimmy Lin
 */
public class DemoWordCountTuple2 extends Configured implements Tool {
	private static final Logger sLogger = Logger.getLogger(DemoWordCountTuple2.class);

	// create the schema for the tuple that will serve as the key
	private static final Schema KEY_SCHEMA = new Schema();

	// define the schema statically
	static {
		KEY_SCHEMA.addField("Token", String.class, "");
		KEY_SCHEMA.addField("EvenOrOdd", Integer.class, new Integer(1));
	}

	// mapper that emits tuple as the key, and value '1' for each occurrence
	private static class MapClass extends Mapper<LongWritable, Tuple, Tuple, IntWritable> {

		// define value '1' statically so we can reuse the object, i.e., avoid
		// unnecessary object creation
		private final static IntWritable one = new IntWritable(1);

		// once again, reuse tuples if possible
		private Tuple tupleOut = KEY_SCHEMA.instantiate();

		@Override
		public void map(LongWritable key, Tuple tupleIn, Context context) throws IOException,
				InterruptedException {

			@SuppressWarnings("unchecked")
			ArrayListWritable<Text> list = (ArrayListWritable<Text>) tupleIn.get(1);

			for (int i = 0; i < list.size(); i++) {
				Text t = (Text) list.get(i);

				String token = t.toString();

				// put new values into the tuple
				tupleOut.set("Token", token);
				tupleOut.set("EvenOrOdd", ((Integer) tupleIn.get(0)) % 2);

				// emit key-value pair
				context.write(tupleOut, one);
			}
		}
	}

	// reducer counts up tuple occurrences
	private static class ReduceClass extends Reducer<Tuple, IntWritable, Tuple, IntWritable> {
		private final static IntWritable SumValue = new IntWritable();

		@Override
		public void reduce(Tuple tupleKey, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			Iterator<IntWritable> iter = values.iterator();

			// sum values
			int sum = 0;
			while (iter.hasNext()) {
				sum += iter.next().get();
			}

			// keep original tuple key, emit sum of counts as value
			SumValue.set(sum);
			context.write(tupleKey, SumValue);
		}
	}

	/**
	 * Creates an instance of this tool.
	 */
	public DemoWordCountTuple2() {
	}

	private static int printUsage() {
		System.out.println("usage: [input-path] [output-path] [num-reducers]");
		ToolRunner.printGenericCommandUsage(System.out);
		return -1;
	}

	/**
	 * Runs this tool.
	 */
	public int run(String[] args) throws Exception {
		if (args.length != 3) {
			printUsage();
			return -1;
		}

		String inputPath = args[0];
		String outputPath = args[1];
		int numReduceTasks = Integer.parseInt(args[2]);

		sLogger.info("Tool: DemoWordCountTuple2");
		sLogger.info(" - input path: " + inputPath);
		sLogger.info(" - output path: " + outputPath);
		sLogger.info(" - number of reducers: " + numReduceTasks);

		Configuration conf = new Configuration();
		Job job = new Job(conf, "DemoWordCountTuple2");
		job.setJarByClass(DemoWordCountTuple2.class);
		job.setNumReduceTasks(numReduceTasks);

		FileInputFormat.setInputPaths(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));

		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		job.setOutputKeyClass(Tuple.class);
		job.setOutputValueClass(IntWritable.class);

		job.setMapperClass(MapClass.class);
		job.setCombinerClass(ReduceClass.class);
		job.setReducerClass(ReduceClass.class);

		// Delete the output directory if it exists already
		Path outputDir = new Path(outputPath);
		FileSystem.get(conf).delete(outputDir, true);

		long startTime = System.currentTimeMillis();
		job.waitForCompletion(true);
		sLogger.info("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0
				+ " seconds");

		return 0;
	}

	/**
	 * Dispatches command-line arguments to the tool via the
	 * <code>ToolRunner</code>.
	 */
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new DemoWordCountTuple2(), args);
		System.exit(res);
	}
}
