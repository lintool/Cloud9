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
public class DemoCountWikipediaPageTypes extends Configured implements Tool {

	private static enum PageTypes {
		TOTAL, REDIRECT, DISAMBIGUATION, EMPTY, ARTICLE, STUB
	};

	private static class MyMapper extends MapReduceBase implements
			Mapper<LongWritable, WikipediaPage, Text, IntWritable> {

		private final static Text sText = new Text();
		private final static IntWritable sInt = new IntWritable(1);
		private DocnoMapping mDocMapping;

		public void configure(JobConf job) {
			try {
				Path[] localFiles = DistributedCache.getLocalCacheFiles(job);

				// instead of hard-coding the actual concrete DocnoMapping
				// class, have the name of the class passed in as a property;
				// this makes the mapper more general
				mDocMapping = (DocnoMapping) Class.forName(job.get("DocnoMappingClass"))
						.newInstance();

				// simply assume that the mappings file is the only file in the
				// distributed cache
				mDocMapping.loadMapping(localFiles[0], FileSystem.getLocal(job));
			} catch (Exception e) {
				e.printStackTrace();
				throw new RuntimeException("Error initializing DocnoMapping!");
			}
		}

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

				sText.set(p.getDocid());
				sInt.set(mDocMapping.getDocno(p.getDocid()));
				output.collect(sText, sInt);
			}
		}
	}

	/**
	 * Creates an instance of this tool.
	 */
	public DemoCountWikipediaPageTypes() {
	}

	private static int printUsage() {
		System.out.println("usage: [input] [output-dir] [mappings-file]");
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
		String mappingFile = args[2];
		int mapTasks = Integer.parseInt(args[3]);

		System.out.println("input dir: " + inputPath);
		System.out.println("output dir: " + outputPath);
		System.out.println("mapping file: " + mappingFile);
		System.out.println("number of mappers: " + mapTasks);

		JobConf conf = new JobConf(DemoCountWikipediaPageTypes.class);
		conf.setJobName("DemoCountWikipediaPageTypes");

		conf.setNumMapTasks(mapTasks);
		conf.setNumReduceTasks(0);

		// pass in the class name as a String; this is makes the mapper general
		// in being able to load any collection of Indexable objects that has
		// docid/docno mapping specified by a DocnoMapping object
		conf.set("DocnoMappingClass", "edu.umd.cloud9.collection.wikipedia.WikipediaDocnoMapping");

		// put the mapping file in the distributed cache so each map worker will
		// have it
		DistributedCache.addCacheFile(new URI(mappingFile), conf);

		FileInputFormat.setInputPaths(conf, new Path(inputPath));
		FileOutputFormat.setOutputPath(conf, new Path(outputPath));
		FileOutputFormat.setCompressOutput(conf, false);

		conf.setInputFormat(WikipediaPageInputFormat.class);
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);

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
		int res = ToolRunner.run(new Configuration(), new DemoCountWikipediaPageTypes(), args);
		System.exit(res);
	}
}
