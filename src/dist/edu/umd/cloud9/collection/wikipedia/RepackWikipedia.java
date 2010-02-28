package edu.umd.cloud9.collection.wikipedia;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
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
 * Program to repack Wikipedia XML dumps into <code>SequenceFiles</code>.
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
 * <li>(block|record|none) to indicate block-compression, record-compression,
 * or no compression</li>
 * </ul>
 * 
 * <p>
 * Here's a sample invocation:
 * </p>
 * 
 * <pre>
 *  hadoop jar cloud9.jar edu.umd.cloud9.collection.wikipedia.RepackWikipedia \
 *  /shared/Wikipedia/raw/enwiki-20100130-pages-articles.xml \
 *  /shared/Wikipedia/compressed.block/en-20100130 \
 *  /shared/Wikipedia/docno-en-20100130.dat block
 * </pre>
 * 
 * 
 * @author Jimmy Lin
 * 
 */
public class RepackWikipedia extends Configured implements Tool {

	private static final Logger sLogger = Logger.getLogger(RepackWikipedia.class);

	private static enum Records {
		TOTAL
	};

	private static class MyMapper extends MapReduceBase implements
			Mapper<LongWritable, WikipediaPage, IntWritable, WikipediaPage> {

		private static final IntWritable sDocno = new IntWritable();
		private WikipediaDocnoMapping mDocnoMapping = new WikipediaDocnoMapping();

		public void configure(JobConf job) {
			try {
				mDocnoMapping.loadMapping(new Path(job.get("DocnoMappingDataFile")), FileSystem
						.get(job));
			} catch (Exception e) {
				throw new RuntimeException("Error loading docno mapping data file!");
			}
		}

		public void map(LongWritable key, WikipediaPage doc,
				OutputCollector<IntWritable, WikipediaPage> output, Reporter reporter)
				throws IOException {
			reporter.incrCounter(Records.TOTAL, 1);

			String id = doc.getDocid();

			if (id != null) {
				reporter.incrCounter(Records.TOTAL, 1);

				sDocno.set(mDocnoMapping.getDocno(id));
				output.collect(sDocno, doc);
			}
		}
	}

	/**
	 * Creates an instance of this tool.
	 */
	public RepackWikipedia() {
	}

	private static int printUsage() {
		System.out
				.println("usage: [xml-dump-file] [output-path] [docno-mapping-data-file] (block|record|none)");
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

		String basePath = args[0];
		String outputPath = args[1];
		String data = args[2];
		String compressionType = args[3];

		if (!compressionType.equals("block") && !compressionType.equals("record")
				&& !compressionType.equals("none")) {
			System.err.println("Error: \"" + compressionType + "\" unknown compression type!");
			System.exit(-1);
		}

		// this is the default block size
		int blocksize = 1000000;

		JobConf conf = new JobConf(RepackWikipedia.class);
		conf.setJobName("RepackWikipedia");

		conf.set("DocnoMappingDataFile", data);

		sLogger.info("Tool name: RepackWikipedia");
		sLogger.info(" - xml dump file: " + basePath);
		sLogger.info(" - output path: " + outputPath);
		sLogger.info(" - docno mapping data file: " + data);
		sLogger.info(" - compression type: " + compressionType);

		if (compressionType.equals("block")) {
			sLogger.info(" - block size: " + blocksize);
		}

		int mapTasks = 10;

		conf.setNumMapTasks(mapTasks);
		conf.setNumReduceTasks(0);

		SequenceFileInputFormat.addInputPath(conf, new Path(basePath));
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

		conf.setInputFormat(WikipediaPageInputFormat.class);
		conf.setOutputFormat(SequenceFileOutputFormat.class);
		conf.setOutputKeyClass(IntWritable.class);
		conf.setOutputValueClass(WikipediaPage.class);

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
		int res = ToolRunner.run(new Configuration(), new RepackWikipedia(), args);
		System.exit(res);
	}

}
