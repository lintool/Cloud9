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
import java.util.Iterator;

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
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

/**
 * <p>
 * Program that builds the mapping from Wikipedia article titles (docids) to
 * sequentially-numbered ints (docnos). The program takes four command-line
 * arguments:
 * </p>
 * 
 * <ul>
 * <li>[input] path to the Wikipedia XML dump file
 * <li>[output-dir] path to temporary MapReduce output directory
 * <li>[output-file] path to location of mappings file
 * <li>[num-mappers] number of mappers to run
 * </ul>
 * 
 * <p>
 * Here's a sample invocation:
 * </p>
 * 
 * <blockquote>
 * 
 * <pre>
 * hadoop jar cloud9.jar edu.umd.cloud9.collection.wikipedia.NumberWikipediaArticles \
 * /umd/collections/wikipedia.raw/enwiki-20081008-pages-articles.xml \
 * /user/jimmylin/wikipedia-docid-tmp \
 * /user/jimmylin/docno.mapping 100
 * </pre>
 * 
 * </blockquote>
 * 
 * @author Jimmy Lin
 */
public class NumberWikipediaArticles extends Configured implements Tool {
	private static final Logger sLogger = Logger.getLogger(NumberWikipediaArticles.class);
	
	private static enum PageTypes {
		TOTAL, REDIRECT, DISAMBIGUATION, EMPTY, ARTICLE, STUB
	};

	private static class MyMapper extends MapReduceBase implements
			Mapper<LongWritable, WikipediaPage, Text, IntWritable> {

		private final static Text sText = new Text();
		private final static IntWritable sInt = new IntWritable(1);

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

				sText.set(p.getTitle());
				output.collect(sText, sInt);
			}
		}
	}

	private static class MyReducer extends MapReduceBase implements
			Reducer<Text, IntWritable, Text, IntWritable> {

		private final static IntWritable sCnt = new IntWritable(1);

		public void reduce(Text key, Iterator<IntWritable> values,
				OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
			output.collect(key, sCnt);
			sCnt.set(sCnt.get() + 1);
		}
	}

	/**
	 * Creates an instance of this tool.
	 */
	public NumberWikipediaArticles() {
	}

	private static int printUsage() {
		System.out.println("usage: [input-path] [output-path] [output-file] [num-mappers]");
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
		String outputFile = args[2];
		int mapTasks = Integer.parseInt(args[3]);

		sLogger.info("input: " + inputPath);
		sLogger.info("output path: " + outputPath);
		sLogger.info("output file: " + outputFile);
		sLogger.info("number of mappers: " + mapTasks);

		JobConf conf = new JobConf(NumberWikipediaArticles.class);
		conf.setJobName("NumberWikipediaArticles");

		conf.setNumMapTasks(mapTasks);
		conf.setNumReduceTasks(1);

		FileInputFormat.setInputPaths(conf, new Path(inputPath));
		FileOutputFormat.setOutputPath(conf, new Path(outputPath));
		FileOutputFormat.setCompressOutput(conf, false);

		conf.setInputFormat(WikipediaPageInputFormat.class);
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);
		conf.setOutputFormat(TextOutputFormat.class);

		conf.setMapperClass(MyMapper.class);
		conf.setReducerClass(MyReducer.class);

		// delete the output directory if it exists already
		FileSystem.get(conf).delete(new Path(outputPath), true);

		JobClient.runJob(conf);

		WikipediaDocnoMapping.writeArticleTitlesData(outputPath + "/part-00000", outputFile);

		return 0;
	}

	/**
	 * Dispatches command-line arguments to the tool via the
	 * <code>ToolRunner</code>.
	 */
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new NumberWikipediaArticles(), args);
		System.exit(res);
	}
}
