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

package edu.umd.cloud9.collection.trecweb;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

/**
 * <p>
 * Program to uncompress the gov2 collection from the original distribution and
 * repack as <code>SequenceFiles</code>.
 * </p>
 * 
 * <p>
 * The program takes three command-line arguments:
 * </p>
 * 
 * <ul>
 * <li>[base-path] base path of the ClueWeb09 distribution</li>
 * <li>[output-path] output path</li>
 * <li>(block|record|none) to indicate block-compression, record-compression,
 * or no compression</li>
 * </ul>
 * 
 * @author Jimmy Lin
 * 
 */
@SuppressWarnings("deprecation")
public class RepackGov2Documents extends Configured implements Tool {

	private static final Logger sLogger = Logger.getLogger(RepackGov2Documents.class);

	private static enum Documents {
		Count
	};

	private static class MyMapper extends MapReduceBase implements
			Mapper<LongWritable, TrecWebDocument, LongWritable, TrecWebDocument> {
		public void map(LongWritable key, TrecWebDocument doc,
				OutputCollector<LongWritable, TrecWebDocument> output, Reporter reporter)
				throws IOException {
			reporter.incrCounter(Documents.Count, 1);

			// sLogger.info("============" + doc.getDocid() + "============" +
			// doc.getContent());

			output.collect(key, doc);
		}
	}

	private RepackGov2Documents() {
	}

	private static int printUsage() {
		System.out.println("usage: [base-path] [output-path] (block|record|none)");
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

		String basePath = args[0];
		String outputPath = args[1];
		String compressionType = args[2];

		if (!compressionType.equals("block") && !compressionType.equals("record")
				&& !compressionType.equals("none")) {
			System.err.println("Error: \"" + compressionType + "\" unknown compression type!");
			System.exit(-1);
		}

		// this is the default block size
		int blocksize = 1000000;

		JobConf conf = new JobConf(RepackGov2Documents.class);
		conf.setJobName("RepackGov2Documents");

		sLogger.info("Tool name: RepackGov2Documents");
		sLogger.info(" - base path: " + basePath);
		sLogger.info(" - output path: " + outputPath);
		sLogger.info(" - compression type: " + compressionType);

		if (compressionType.equals("block")) {
			sLogger.info(" - block size: " + blocksize);
		}

		int mapTasks = 10;

		conf.setNumMapTasks(mapTasks);
		conf.setNumReduceTasks(500);

		// 272
		for (int i = 0; i <= 272; i++) {
			String path = basePath + "/GX";
			String indexNum = Integer.toString(i);
			if (indexNum.length() == 1) {
				path += "00";
			}
			if (indexNum.length() == 2) {
				path += "0";
			}
			path += indexNum;
			FileInputFormat.addInputPath(conf, new Path(path));
		}

		SequenceFileOutputFormat.setOutputPath(conf, new Path(outputPath));

		if (compressionType.equals("none")) {
			SequenceFileOutputFormat.setCompressOutput(conf, false);
		} else {
			SequenceFileOutputFormat.setCompressOutput(conf, true);

			if (compressionType.equals("record")) {
				SequenceFileOutputFormat.setOutputCompressionType(conf,
						SequenceFile.CompressionType.RECORD);
			} else {
				SequenceFileOutputFormat.setOutputCompressionType(conf,
						SequenceFile.CompressionType.BLOCK);
				conf.setInt("io.seqfile.compress.blocksize", blocksize);
			}
		}

		conf.setInputFormat(TrecWebDocumentInputFormat.class);
		conf.setOutputFormat(SequenceFileOutputFormat.class);
		conf.setOutputKeyClass(LongWritable.class);
		conf.setOutputValueClass(TrecWebDocument.class);

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
		int res = ToolRunner.run(new Configuration(), new RepackGov2Documents(), args);
		System.exit(res);
	}

}
