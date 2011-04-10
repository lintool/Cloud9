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

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

/**
 * <p>
 * Tool for repacking Wikipedia XML dumps into <code>SequenceFiles</code>.
 * </p>
 * 
 * <p>
 * The program takes the following command-line arguments:
 * </p>
 * 
 * <ul>
 * <li>[xml-dump-file] XML dump file</li>
 * <li>[output-path] output path</li>
 * <li>[docno-mapping-data-file] docno mapping data file</li>
 * <li>(block|record|none) to indicate block-compression, record-compression, or
 * no compression</li>
 * </ul>
 * 
 * <p>
 * Here's a sample invocation:
 * </p>
 * 
 * <pre>
 * hadoop jar cloud9.jar edu.umd.cloud9.collection.wikipedia.RepackWikipedia \
 *   -libjars bliki-core-3.0.15.jar,commons-lang-2.5.jar \
 *   /user/jimmy/Wikipedia/raw/enwiki-20101011-pages-articles.xml \
 *   /user/jimmy/Wikipedia/compressed.block/en-20101011 \
 *   /user/jimmy/Wikipedia/docno-en-20101011.dat block
 * </pre>
 * 
 * 
 * @author Jimmy Lin
 * 
 */
@SuppressWarnings("deprecation")
public class RepackWikipedia extends Configured implements Tool {
	private static final Logger LOG = Logger.getLogger(RepackWikipedia.class);

	private static enum Records { TOTAL	};

	private static class MyMapper extends MapReduceBase implements
			Mapper<LongWritable, WikipediaPage, IntWritable, WikipediaPage> {

		private static final IntWritable docno = new IntWritable();
		private static final WikipediaDocnoMapping docnoMapping = new WikipediaDocnoMapping();

		public void configure(JobConf job) {
			try {
				docnoMapping.loadMapping(new Path(job.get("DocnoMappingDataFile")), FileSystem.get(job));
			} catch (Exception e) {
				throw new RuntimeException("Error loading docno mapping data file!");
			}
		}

		public void map(LongWritable key, WikipediaPage doc,
				OutputCollector<IntWritable, WikipediaPage> output, Reporter reporter) throws IOException {
			reporter.incrCounter(Records.TOTAL, 1);
			String id = doc.getDocid();

			if (id != null) {
				int no = docnoMapping.getDocno(id);
				if(no >= 0){
					docno.set(no);
					output.collect(docno, doc);
				}
			}
		}
	}

	/**
	 * Runs this tool.
	 */
	public int run(String[] args) throws Exception {
		if (args.length != 4) {
			System.out.println("usage: [xml-dump-file] [output-path] [docno-mapping-data-file] (block|record|none)");
			ToolRunner.printGenericCommandUsage(System.out);
			return -1;
		}

		String basePath = args[0];
		String outputPath = args[1];
		String data = args[2];
		String compressionType = args[3];

		if (!"block".equals(compressionType) && !"record".equals(compressionType) && !"none".equals(compressionType)) {
			System.err.println("Error: \"" + compressionType + "\" unknown compression type!");
			System.exit(-1);
		}

		// this is the default block size
		int blocksize = 1000000;

		JobConf conf = new JobConf(getConf(), RepackWikipedia.class);
		conf.setJobName("RepackWikipedia");

		conf.set("DocnoMappingDataFile", data);

		LOG.info("Tool name: RepackWikipedia");
		LOG.info(" - xml dump file: " + basePath);
		LOG.info(" - output path: " + outputPath);
		LOG.info(" - docno mapping data file: " + data);
		LOG.info(" - compression type: " + compressionType);

		if ("block".equals(compressionType)) {
			LOG.info(" - block size: " + blocksize);
		}

		int mapTasks = 10;

		conf.setNumMapTasks(mapTasks);
		conf.setNumReduceTasks(0);

		SequenceFileInputFormat.addInputPath(conf, new Path(basePath));
		SequenceFileOutputFormat.setOutputPath(conf, new Path(outputPath));

		if ("none".equals(compressionType)) {
			SequenceFileOutputFormat.setCompressOutput(conf, false);
		} else {
			SequenceFileOutputFormat.setCompressOutput(conf, true);

			if ("record".equals(compressionType)) {
				SequenceFileOutputFormat.setOutputCompressionType(conf, SequenceFile.CompressionType.RECORD);
			} else {
				SequenceFileOutputFormat.setOutputCompressionType(conf,	SequenceFile.CompressionType.BLOCK);
				conf.setInt("io.seqfile.compress.blocksize", blocksize);
			}
		}

		conf.setInputFormat(WikipediaPageInputFormat.class);
		conf.setOutputFormat(SequenceFileOutputFormat.class);
		conf.setOutputKeyClass(IntWritable.class);
		conf.setOutputValueClass(WikipediaPage.class);

		conf.setMapperClass(MyMapper.class);

		// Delete the output directory if it exists already.
		FileSystem.get(conf).delete(new Path(outputPath), true);

		JobClient.runJob(conf);

		return 0;
	}

	public RepackWikipedia() {}

	public static void main(String[] args) throws Exception {
		ToolRunner.run(new RepackWikipedia(), args);
	}
}
