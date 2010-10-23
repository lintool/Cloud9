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
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapRunnable;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.lib.IdentityReducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import edu.umd.cloud9.mapred.NoSplitSequenceFileInputFormat;
import edu.umd.cloud9.util.FSLineReader;

/**
 * <p>
 * Tool for building a document forward index for Wikipedia. Sample invocation:
 * </p>
 *
 * <pre>
 * hadoop jar cloud9.jar edu.umd.cloud9.collection.wikipedia.BuildWikipediaForwardIndex \
 *   -libjars bliki-core-3.0.15.jar,commons-lang-2.5.jar \
 *   /user/jimmy/Wikipedia/compressed.block/en-20101011 tmp \
 *   /user/jimmy/Wikipedia/compressed.block/findex-en-20101011.dat
 * </pre>
 * 
 * @author Jimmy Lin
 * 
 */
public class BuildWikipediaForwardIndex extends Configured implements Tool {

	private static final Logger sLogger = Logger.getLogger(BuildWikipediaForwardIndex.class);

	private static enum Blocks {
		Total
	};

	private static class MyMapRunner implements
			MapRunnable<IntWritable, WikipediaPage, IntWritable, Text> {

		int fileno;
		long pos = -1;

		private static final IntWritable sOutputKey = new IntWritable();
		private static final Text sOutputValue = new Text();

		@SuppressWarnings("unchecked")
		public void configure(JobConf job) {
			String file = job.get("map.input.file");
			fileno = Integer.parseInt(file.substring(file.indexOf("part-") + 5));
		}

		public void run(RecordReader<IntWritable, WikipediaPage> input,
				OutputCollector<IntWritable, Text> output, Reporter reporter) throws IOException {

			IntWritable key = new IntWritable();
			WikipediaPage value = new WikipediaPage();

			long pos = -1;
			long prevPos = -1;

			int prevDocno = 0;

			pos = input.getPos();
			while (input.next(key, value)) {
				if (prevPos != -1 && prevPos != pos) {
					sLogger.info("- beginning of block at " + prevPos + ", docno:" + prevDocno
							+ ", file:" + fileno);
					sOutputKey.set(prevDocno);
					sOutputValue.set(prevPos + "\t" + fileno);
					output.collect(sOutputKey, sOutputValue);
					reporter.incrCounter(Blocks.Total, 1);
				}

				prevPos = pos;
				pos = input.getPos();
				prevDocno = key.get();
			}

		}

	}

	public BuildWikipediaForwardIndex() {
	}

	private static int printUsage() {
		System.out.println("usage: [collection-path] [output-path] [index-file]");
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

		JobConf conf = new JobConf(getConf(), BuildWikipediaForwardIndex.class);
		FileSystem fs = FileSystem.get(conf);

		String collectionPath = args[0];
		String outputPath = args[1];
		String indexFile = args[2];

		sLogger.info("Tool name: BuildWikipediaForwardIndex");
		sLogger.info(" - collection path: " + collectionPath);
		sLogger.info(" - output path: " + outputPath);
		sLogger.info(" - index file: " + indexFile);
		sLogger.info("Note: This tool only works on block-compressed SequenceFiles!");

		conf.setJobName("BuildWikipediaForwardIndex:" + collectionPath);

		conf.setNumMapTasks(100);
		conf.setNumReduceTasks(1);

		FileInputFormat.setInputPaths(conf, new Path(collectionPath));
		FileOutputFormat.setOutputPath(conf, new Path(outputPath));
		FileOutputFormat.setCompressOutput(conf, false);

		conf.setInputFormat(NoSplitSequenceFileInputFormat.class);
		conf.setOutputKeyClass(IntWritable.class);
		conf.setOutputValueClass(Text.class);

		conf.setMapRunnerClass(MyMapRunner.class);
		conf.setReducerClass(IdentityReducer.class);

		// delete the output directory if it exists already
		fs.delete(new Path(outputPath), true);

		RunningJob job = JobClient.runJob(conf);

		Counters counters = job.getCounters();
		int blocks = (int) counters.findCounter(Blocks.Total).getCounter();

		sLogger.info("number of blocks: " + blocks);

		sLogger.info("Writing index file...");
		FSLineReader reader = new FSLineReader(outputPath + "/part-00000", fs);
		FSDataOutputStream out = fs.create(new Path(indexFile), true);

		out.writeUTF("edu.umd.cloud9.collection.wikipedia.WikipediaForwardIndex");
		out.writeUTF(collectionPath);
		out.writeInt(blocks);

		int cnt = 0;
		Text line = new Text();
		while (reader.readLine(line) > 0) {
			String[] arr = line.toString().split("\\s+");

			int docno = Integer.parseInt(arr[0]);
			int offset = Integer.parseInt(arr[1]);
			short fileno = Short.parseShort(arr[2]);

			out.writeInt(docno);
			out.writeInt(offset);
			out.writeShort(fileno);

			cnt++;

			if (cnt % 100000 == 0) {
				sLogger.info(cnt + " blocks written");
			}

		}

		reader.close();
		out.close();

		if (cnt != blocks) {
			throw new RuntimeException("Error: mismatch in block count!");
		}

		return 0;
	}

	/**
	 * Dispatches command-line arguments to the tool via the
	 * <code>ToolRunner</code>.
	 */
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new BuildWikipediaForwardIndex(), args);
		System.exit(res);
	}
}