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

package edu.umd.cloud9.indexer;

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
import org.apache.hadoop.mapred.MapFileOutputFormat;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.lib.IdentityReducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import edu.umd.cloud9.io.ArrayListWritable;
import edu.umd.cloud9.io.PairOfInts;
import edu.umd.cloud9.util.Histogram;
import edu.umd.cloud9.util.MapKI;

public class BuildInvertedIndex extends Configured implements Tool {

	private static final Logger sLogger = Logger.getLogger(BuildInvertedIndex.class);

	private static class MyMapper extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, PairOfInts> {

		private static final Text word = new Text();
		private Histogram<String> termCounts = new Histogram<String>();

		public void map(LongWritable docno, Text doc, OutputCollector<Text, PairOfInts> output,
				Reporter reporter) throws IOException {
			String text = doc.toString();
			termCounts.clear();

			String[] terms = text.split("\\s+");

			// first build a histogram of the terms
			for (String term : terms) {
				if (term == null || term.length() == 0)
					continue;

				termCounts.count(term);
			}

			// emit postings
			for (MapKI.Entry<String> e : termCounts.entrySet()) {
				word.set(e.getKey());
				output.collect(word, new PairOfInts((int) docno.get(), e.getValue()));
			}
		}
	}

	private static class MyReducer extends MapReduceBase implements
			Reducer<Text, PairOfInts, Text, ArrayListWritable<PairOfInts>> {

		public void reduce(Text key, Iterator<PairOfInts> values,
				OutputCollector<Text, ArrayListWritable<PairOfInts>> output, Reporter reporter)
				throws IOException {
			ArrayListWritable<PairOfInts> postings = new ArrayListWritable<PairOfInts>();

			while (values.hasNext()) {
				postings.add(values.next().clone());
			}
			output.collect(key, postings);
		}
	}

	private BuildInvertedIndex() {
	}

	private static int printUsage() {
		System.out.println("usage: [input-path] [output-path] [num-mappers]");
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
		int mapTasks = Integer.parseInt(args[2]);
		int reduceTasks = 1;

		sLogger.info("Tool name: BuildInvertedIndex");
		sLogger.info(" - input path: " + inputPath);
		sLogger.info(" - output path: " + outputPath);
		sLogger.info(" - num mappers: " + mapTasks);
		sLogger.info(" - num reducers: " + reduceTasks);

		JobConf conf = new JobConf(BuildInvertedIndex.class);
		conf.setJobName("BuildInvertIndex");

		conf.setNumMapTasks(mapTasks);
		conf.setNumReduceTasks(reduceTasks);

		FileInputFormat.setInputPaths(conf, new Path(inputPath));
		FileOutputFormat.setOutputPath(conf, new Path(outputPath));

		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(PairOfInts.class);
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(ArrayListWritable.class);
		conf.setOutputFormat(MapFileOutputFormat.class);

		conf.setMapperClass(MyMapper.class);
		conf.setCombinerClass(IdentityReducer.class);
		conf.setReducerClass(MyReducer.class);

		// Delete the output directory if it exists already
		Path outputDir = new Path(outputPath);
		FileSystem.get(conf).delete(outputDir, true);

		JobClient.runJob(conf);

		return 0;
	}

	/**
	 * Dispatches command-line arguments to the tool via the
	 * <code>ToolRunner</code>.
	 */
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new BuildInvertedIndex(), args);
		System.exit(res);
	}

}
