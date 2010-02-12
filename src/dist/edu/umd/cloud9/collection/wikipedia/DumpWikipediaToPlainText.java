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
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
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

import edu.umd.cloud9.collection.DocnoMapping;

/**
 * <p>
 * Simple demo program that counts the number of pages in a particular Wikipedia
 * XML dump file. This program keeps track of total number of pages, redirect
 * pages, disambiguation pages, empty pages, actual articles (including stubs),
 * and stubs. This also provides a skeleton for MapReduce programs to process
 * the collection. The program takes three command-line arguments:
 * </p>
 * 
 * <ul>
 * <li>[input] path to the Wikipedia XML dump file
 * <li>[output-dir] path to the output directory
 * <li>[mappings-file] path to the docno mappings file
 * </ul>
 * 
 * <p>
 * Here's a sample invocation:
 * </p>
 * 
 * <blockquote>
 * 
 * <pre>
 * hadoop jar cloud9.jar edu.umd.cloud9.collection.wikipedia.DemoCountWikipediaPageTypes \
 * /umd/collections/wikipedia.raw/enwiki-20081008-pages-articles.xml \
 * /user/jimmylin/count-tmp \
 * /user/jimmylin/docno.mapping
 * </pre>
 * 
 * </blockquote>
 * 
 * @author Jimmy Lin
 */
public class DumpWikipediaToPlainText extends Configured implements Tool {

	private static enum PageTypes {
		TOTAL, REDIRECT, DISAMBIGUATION, EMPTY, ARTICLE, STUB
	};

	private static class MyMapper extends MapReduceBase implements
			Mapper<LongWritable, WikipediaPage, Text, Text> {

		private final static Text sTitle = new Text();
		private final static Text sPage = new Text();

		public void map(LongWritable key, WikipediaPage p,
				OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
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

				sTitle.set(p.getTitle());
				sPage.set(MarkupStripper.stripEverything(p.getContent()));
			
				output.collect(sTitle, sPage);
			}
		}
	}

	/**
	 * Creates an instance of this tool.
	 */
	public DumpWikipediaToPlainText() {
	}

	private static int printUsage() {
		System.out.println("usage: [input] [output-dir] [num-mappers]");
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

		System.out.println("input dir: " + inputPath);
		System.out.println("output dir: " + outputPath);
		System.out.println("number of mappers: " + mapTasks);

		JobConf conf = new JobConf(DumpWikipediaToPlainText.class);
		conf.setJobName("DumpWikipediaToPlainText");

		conf.setNumMapTasks(mapTasks);
		conf.setNumReduceTasks(0);

		FileInputFormat.setInputPaths(conf, new Path(inputPath));
		FileOutputFormat.setOutputPath(conf, new Path(outputPath));
		FileOutputFormat.setCompressOutput(conf, false);

		conf.setInputFormat(WikipediaPageInputFormat.class);
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);

		conf.setMapperClass(MyMapper.class);

		// delete the output directory if it exists already
		FileSystem.get(conf).delete(new Path(outputPath), true);

		JobClient.runJob(conf);

		return 0;
	}

	/**
	 * Dispatches command-line arguments to the tool via the
	 * <code>ToolRunner</code>.
	 */
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new DumpWikipediaToPlainText(), args);
		System.exit(res);
	}
}
