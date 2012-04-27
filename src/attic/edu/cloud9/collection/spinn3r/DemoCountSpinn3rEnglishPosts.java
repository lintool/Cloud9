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

package edu.umd.cloud9.collection.spinn3r;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

public class DemoCountSpinn3rEnglishPosts extends Configured implements Tool {

	private static final Logger sLogger = Logger.getLogger(DemoCountSpinn3rEnglishPosts.class);

	private static enum Languages {
		TOTAL, EN
	};

	private static class MyMapper extends MapReduceBase implements
			Mapper<LongWritable, Spinn3rItem, LongWritable, Text> {
		public void map(LongWritable key, Spinn3rItem item,
				OutputCollector<LongWritable, Text> output, Reporter reporter) throws IOException {

			if (item.getLanguage().equals("en")) {
				reporter.incrCounter(Languages.EN, 1);
			}
			reporter.incrCounter(Languages.TOTAL, 1);

		}
	}

	/**
	 * Creates an instance of this tool.
	 */
	public DemoCountSpinn3rEnglishPosts() {
	}

	private static int printUsage() {
		System.out.println("usage: [input] [output-dir]");
		ToolRunner.printGenericCommandUsage(System.out);
		return -1;
	}

	/**
	 * Runs this tool.
	 */
	public int run(String[] args) throws Exception {
		if (args.length != 2) {
			printUsage();
			return -1;
		}

		String inputPath = args[0];
		String outputPath = args[1];

		sLogger.info("input dir: " + inputPath);
		sLogger.info("output dir: " + outputPath);

		JobConf conf = new JobConf(DemoCountSpinn3rEnglishPosts.class);
		conf.setJobName("DemoCountSpinn3rEnglishPosts");

		conf.setNumReduceTasks(0);

		FileInputFormat.setInputPaths(conf, new Path(inputPath));
		FileOutputFormat.setOutputPath(conf, new Path(outputPath));
		FileOutputFormat.setCompressOutput(conf, false);

		conf.setInputFormat(Spinn3rItemInputFormat.class);
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);

		conf.setMapperClass(MyMapper.class);

		// delete the output directory if it exists already
		FileSystem.get(conf).delete(new Path(outputPath), true);

		JobClient.runJob(conf);

		// clean up
		FileSystem.get(conf).delete(new Path(outputPath), true);

		return 0;
	}

	/**
	 * Dispatches command-line arguments to the tool via the
	 * <code>ToolRunner</code>.
	 */
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new DemoCountSpinn3rEnglishPosts(), args);
		System.exit(res);
	}
}
