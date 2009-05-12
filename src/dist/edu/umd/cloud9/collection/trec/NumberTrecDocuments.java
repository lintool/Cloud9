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

package edu.umd.cloud9.collection.trec;

import java.io.IOException;
import java.util.Iterator;

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

/**
 * <p>
 * Program that builds the mapping from TREC docids (String identifiers) to
 * docnos (sequentially-numbered ints). The program takes four command-line
 * arguments:
 * </p>
 * 
 * <ul>
 * <li>[input-dir] path to the document collection
 * <li>[output-dir] path to temporary MapReduce output directory
 * <li>[output-file] path to location of mapping file
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
 * hadoop jar cloud9.jar edu.umd.cloud9.collection.trec.NumberTrecDocuments \
 * /umd/collections/trec/trec4-5_noCR.xml \
 * /user/jimmylin/trec-docid-tmp \
 * /user/jimmylin/docno.mapping 100
 * </pre>
 * 
 * </blockquote>
 * 
 */
public class NumberTrecDocuments {

	protected static enum Count {
		DOCS
	};

	private static class MyMapper extends MapReduceBase implements
			Mapper<LongWritable, TrecDocument, Text, IntWritable> {

		private final static Text sText = new Text();
		private final static IntWritable sInt = new IntWritable(1);

		public void map(LongWritable key, TrecDocument doc,
				OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
			reporter.incrCounter(Count.DOCS, 1);

			sText.set(doc.getDocid());
			output.collect(sText, sInt);
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

	private NumberTrecDocuments() {
	}

	/**
	 * Runs the program
	 * 
	 * @param args
	 *            command-line arguments
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {
		if (args.length != 4) {
			System.out.println("usage: [input-dir] [output-dir] [output-file] [num-mappers]");
			System.exit(-1);
		}

		String inputPath = args[0];
		String outputPath = args[1];
		String outputFile = args[2];
		int mapTasks = Integer.parseInt(args[3]);

		System.out.println("input dir: " + inputPath);
		System.out.println("output dir: " + outputPath);
		System.out.println("output file: " + outputFile);
		System.out.println("number of mappers: " + mapTasks);

		JobConf conf = new JobConf(NumberTrecDocuments.class);
		conf.setJobName("NumberTrecDocuments");

		conf.setNumMapTasks(mapTasks);
		conf.setNumReduceTasks(1);

		FileInputFormat.setInputPaths(conf, new Path(inputPath));
		FileOutputFormat.setOutputPath(conf, new Path(outputPath));
		FileOutputFormat.setCompressOutput(conf, false);

		conf.setInputFormat(TrecDocumentInputFormat.class);
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);
		conf.setOutputFormat(TextOutputFormat.class);

		conf.setMapperClass(MyMapper.class);
		conf.setReducerClass(MyReducer.class);

		// delete the output directory if it exists already
		FileSystem.get(conf).delete(new Path(outputPath), true);

		JobClient.runJob(conf);

		TrecDocnoMapping.writeDocnoData(outputPath + "/part-00000", outputFile);
	}
}
