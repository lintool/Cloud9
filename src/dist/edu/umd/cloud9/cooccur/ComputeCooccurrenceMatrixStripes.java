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

package edu.umd.cloud9.cooccur;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import edu.umd.cloud9.io.HMapSIW;

/**
 * <p>
 * Implementation of the "stripes" algorithm for computing co-occurrence
 * matrices from corpora. Algorithm is described in:
 * </p>
 * 
 * <blockquote>Jimmy Lin. <b>Scalable Language Processing Algorithms for the
 * Masses: A Case Study in Computing Word Co-occurrence Matrices with MapReduce.</b>
 * <i>Proceedings of the 2008 Conference on Empirical Methods in Natural
 * Language Processing (EMNLP 2008).</i></blockquote>
 * 
 * @author Jimmy Lin
 */
public class ComputeCooccurrenceMatrixStripes extends Configured implements Tool {
	private static final Logger sLogger = Logger.getLogger(ComputeCooccurrenceMatrixStripes.class);

	private static class MyMapper extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, HMapSIW> {

		private int window = 2;
		private HMapSIW map = new HMapSIW();
		private Text textKey = new Text();

		public void configure(JobConf job) {
			window = job.getInt("window", 2);
		}

		public void map(LongWritable key, Text line, OutputCollector<Text, HMapSIW> output,
				Reporter reporter) throws IOException {
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
				output.collect(textKey, map);
			}
		}
	}

	private static class MyReducer extends MapReduceBase implements
			Reducer<Text, HMapSIW, Text, HMapSIW> {

		public void reduce(Text key, Iterator<HMapSIW> values,
				OutputCollector<Text, HMapSIW> output, Reporter reporter) throws IOException {

			HMapSIW map = new HMapSIW();

			while (values.hasNext()) {
				map.plus(values.next());
			}

			output.collect(key, map);
		}
	}

	/**
	 * Creates an instance of this tool.
	 */
	public ComputeCooccurrenceMatrixStripes() {
	}

	private static int printUsage() {
		System.out
				.println("usage: [input-path] [output-path] [window] [num-mappers] [num-reducers]");
		ToolRunner.printGenericCommandUsage(System.out);
		return -1;
	}

	/**
	 * Runs this tool.
	 */
	public int run(String[] args) throws Exception {
		if (args.length != 5) {
			printUsage();
			return -1;
		}

		String inputPath = args[0];
		String outputPath = args[1];

		int window = Integer.parseInt(args[2]);
		int mapTasks = Integer.parseInt(args[3]);
		int reduceTasks = Integer.parseInt(args[4]);

		sLogger.info("Tool: ComputeCooccurrenceMatrixStripes");
		sLogger.info(" - input path: " + inputPath);
		sLogger.info(" - output path: " + outputPath);
		sLogger.info(" - window: " + window);
		sLogger.info(" - number of mappers: " + mapTasks);
		sLogger.info(" - number of reducers: " + reduceTasks);

		JobConf conf = new JobConf(ComputeCooccurrenceMatrixStripes.class);

		// Delete the output directory if it exists already
		Path outputDir = new Path(outputPath);
		FileSystem.get(conf).delete(outputDir, true);

		conf.setJobName("CooccurrenceMatrixStripes");

		conf.setInt("window", window);
		conf.setNumMapTasks(mapTasks);
		conf.setNumReduceTasks(reduceTasks);

		FileInputFormat.setInputPaths(conf, new Path(inputPath));

		FileOutputFormat.setCompressOutput(conf, false);
		FileOutputFormat.setOutputPath(conf, new Path(outputPath));

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(HMapSIW.class);

		conf.setMapperClass(MyMapper.class);
		conf.setCombinerClass(MyReducer.class);
		conf.setReducerClass(MyReducer.class);

		long startTime = System.currentTimeMillis();
		JobClient.runJob(conf);
		System.out.println("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0
				+ " seconds");

		return 0;
	}

	/**
	 * Dispatches command-line arguments to the tool via the
	 * <code>ToolRunner</code>.
	 */
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new ComputeCooccurrenceMatrixStripes(), args);
		System.exit(res);
	}
}
