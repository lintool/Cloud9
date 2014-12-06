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

package edu.umd.cloud9.collection.line;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.lib.NullOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

public class DemoCountTextDocuments extends Configured implements Tool {

	private static final Logger sLogger = Logger.getLogger(DemoCountTextDocuments.class);

	private static enum Count {
		DOCS
	};

	private static class MyMapper extends MapReduceBase implements
			Mapper<LongWritable, TextDocument, NullWritable, NullWritable> {

		public void configure(JobConf job) {
		}

		public void map(LongWritable key, TextDocument doc,
				OutputCollector<NullWritable, NullWritable> output, Reporter reporter)
				throws IOException {
			reporter.incrCounter(Count.DOCS, 1);
		}
	}

	/**
	 * Creates an instance of this tool.
	 */
	public DemoCountTextDocuments() {
	}

	private static int printUsage() {
		System.out.println("usage: [input]");
		ToolRunner.printGenericCommandUsage(System.out);
		return -1;
	}

	/**
	 * Runs this tool.
	 */
	public int run(String[] args) throws Exception {
		if (args.length != 1) {
			printUsage();
			return -1;
		}

		String inputPath = args[0];

		sLogger.info("input: " + inputPath);

		JobConf conf = new JobConf(DemoCountTextDocuments.class);
		conf.setJobName("DemoCountTextDocuments");

		conf.setNumReduceTasks(0);

		FileInputFormat.setInputPaths(conf, new Path(inputPath));

		conf.setInputFormat(TextDocumentInputFormat.class);
		conf.setOutputFormat(NullOutputFormat.class);
		conf.setMapperClass(MyMapper.class);

		JobClient.runJob(conf);

		return 0;
	}

	/**
	 * Dispatches command-line arguments to the tool via the
	 * <code>ToolRunner</code>.
	 */
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new DemoCountTextDocuments(), args);
		System.exit(res);
	}
}
