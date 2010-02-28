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

package edu.umd.cloud9.collection.wikipedia;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
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

/**
 * <p>
 * Simple demo program that counts the number of pages in a particular Wikipedia
 * XML dump file. This program keeps track of total number of pages, redirect
 * pages, disambiguation pages, empty pages, actual articles (including stubs),
 * and stubs. This also provides a skeleton for MapReduce programs to process
 * the collection. The program takes a single command-line argument, which is
 * the path to the Wikipedia XML dump file.
 * </p>
 * 
 * <p>
 * Here's a sample invocation:
 * </p>
 * 
 * <blockquote>
 * 
 * <pre>
 * hadoop jar cloud9.jar edu.umd.cloud9.collection.wikipedia.DemoCountWikipediaPageTypes \
 * /shared/Wikipedia/raw/enwiki-20091202-pages-articles.xml
 * </pre>
 * 
 * </blockquote>
 * 
 * @author Jimmy Lin
 */
public class DemoCountWikipediaPages extends Configured implements Tool {

	private static final Logger sLogger = Logger.getLogger(DemoCountWikipediaPages.class);

	private static enum PageTypes {
		TOTAL, REDIRECT, DISAMBIGUATION, EMPTY, ARTICLE, STUB
	};

	private static class MyMapper extends MapReduceBase implements
			Mapper<LongWritable, WikipediaPage, Text, IntWritable> {

		public void map(LongWritable key, WikipediaPage p,
				OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
			reporter.incrCounter(PageTypes.TOTAL, 1);

			if (p.isRedirect()) {
				reporter.incrCounter(PageTypes.REDIRECT, 1);

			} else if (p.isDisambiguation()) {
				reporter.incrCounter(PageTypes.DISAMBIGUATION, 1);
			} else if (p.isEmpty()) {
				reporter.incrCounter(PageTypes.EMPTY, 1);
			} else {
				reporter.incrCounter(PageTypes.ARTICLE, 1);

				if (p.isStub()) {
					reporter.incrCounter(PageTypes.STUB, 1);
				}
			}
		}
	}

	/**
	 * Creates an instance of this tool.
	 */
	public DemoCountWikipediaPages() {
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

		sLogger.info("Tool name: RepackWikipedia");
		sLogger.info(" - xml dump file: " + inputPath);

		JobConf conf = new JobConf(DemoCountWikipediaPages.class);
		conf.setJobName("DemoCountWikipediaPages");

		conf.setNumMapTasks(10);
		conf.setNumReduceTasks(0);

		FileInputFormat.setInputPaths(conf, new Path(inputPath));

		conf.setInputFormat(WikipediaPageInputFormat.class);
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
		int res = ToolRunner.run(new Configuration(), new DemoCountWikipediaPages(), args);
		System.exit(res);
	}
}
