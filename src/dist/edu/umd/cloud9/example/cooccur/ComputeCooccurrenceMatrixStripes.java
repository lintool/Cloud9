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

package edu.umd.cloud9.example.cooccur;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import edu.umd.cloud9.io.fastuil.String2IntOpenHashMapWritable;

/**
 * <p>
 * Implementation of the "stripes" algorithm for computing co-occurrence
 * matrices from a large text collection. This algorithm is described in Chapter
 * 3 of "Data-Intensive Text Processing with MapReduce" by Lin &amp; Dyer, as
 * well as the following paper:
 * </p>
 * 
 * <blockquote>Jimmy Lin. <b>Scalable Language Processing Algorithms for the
 * Masses: A Case Study in Computing Word Co-occurrence Matrices with
 * MapReduce.</b>
 * <i>Proceedings of the 2008 Conference on Empirical Methods in Natural
 * Language Processing (EMNLP 2008)</i>, pages 419-428.</blockquote>
 * 
 * <p>
 * This program takes the following command-line arguments:
 * </p>
 * 
 * <ul>
 * <li>[input-path]</li>
 * <li>[output-path]</li>
 * <li>[window]</li>
 * <li>[num-reducers]</li>
 * </ul>
 * 
 * @author Jimmy Lin
 */
public class ComputeCooccurrenceMatrixStripes extends Configured implements Tool {
	private static final Logger sLogger = Logger.getLogger(ComputeCooccurrenceMatrixStripes.class);

	private static class MyMapper extends
			Mapper<LongWritable, Text, Text, String2IntOpenHashMapWritable> {

		private int window = 2;
		private String2IntOpenHashMapWritable map = new String2IntOpenHashMapWritable();
		private Text textKey = new Text();

		@Override
		public void setup(Context context) {
			window = context.getConfiguration().getInt("window", 2);
		}

		@Override
		public void map(LongWritable key, Text line, Context context) throws IOException,
				InterruptedException {
			String text = line.toString();

			String[] terms = text.split("\\s+");

			for (int i = 0; i < terms.length; i++) {
				String term = terms[i];

				// skip empty tokens
				if (term.length() == 0)
					continue;

				map.clear();

				for (int j = i - window; j < i + window + 1; j++) {
					if (j == i || j < 0)
						continue;

					if (j >= terms.length)
						break;

					// skip empty tokens
					if (terms[j].length() == 0)
						continue;

					if (map.containsKey(terms[j])) {
						map.increment(terms[j]);
					} else {
						map.put(terms[j], 1);
					}
				}

				textKey.set(term);
				context.write(textKey, map);
			}
		}
	}

	private static class MyReducer extends
			Reducer<Text, String2IntOpenHashMapWritable, Text, String2IntOpenHashMapWritable> {

		@Override
		public void reduce(Text key, Iterable<String2IntOpenHashMapWritable> values, Context context)
				throws IOException,
				InterruptedException {
			Iterator<String2IntOpenHashMapWritable> iter = values.iterator();

			String2IntOpenHashMapWritable map = new String2IntOpenHashMapWritable();

			while (iter.hasNext()) {
				map.plus(iter.next());
			}

			context.write(key, map);
		}
	}

	/**
	 * Creates an instance of this tool.
	 */
	public ComputeCooccurrenceMatrixStripes() {
	}

	private static int printUsage() {
		System.out
				.println("usage: [input-path] [output-path] [window] [num-reducers]");
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

		int window = Integer.parseInt(args[2]);
		int reduceTasks = Integer.parseInt(args[3]);

		sLogger.info("Tool: ComputeCooccurrenceMatrixStripes");
		sLogger.info(" - input path: " + inputPath);
		sLogger.info(" - output path: " + outputPath);
		sLogger.info(" - window: " + window);
		sLogger.info(" - number of reducers: " + reduceTasks);

		Job job = new Job(getConf(), "CooccurrenceMatrixStripes");

		// Delete the output directory if it exists already
		Path outputDir = new Path(outputPath);
		FileSystem.get(getConf()).delete(outputDir, true);

		job.getConfiguration().setInt("window", window);

		job.setJarByClass(ComputeCooccurrenceMatrixStripes.class);
		job.setNumReduceTasks(reduceTasks);

		FileInputFormat.setInputPaths(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(String2IntOpenHashMapWritable.class);

		job.setMapperClass(MyMapper.class);
		job.setCombinerClass(MyReducer.class);
		job.setReducerClass(MyReducer.class);

		long startTime = System.currentTimeMillis();
		job.waitForCompletion(true);
		System.out.println("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");

		return 0;
	}

	/**
	 * Dispatches command-line arguments to the tool via the
	 * <code>ToolRunner</code>.
	 */
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new ComputeCooccurrenceMatrixStripes(), args);
		System.exit(res);
	}
}
