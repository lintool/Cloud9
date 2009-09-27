package edu.umd.cloud9.collection.clue;

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
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

/**
 * <p>
 * Program to uncompress the ClueWeb09 collection from the original distribution
 * WARC files and repack as block compressed <code>SequenceFiles</code>.
 * </p>
 * 
 * <p>
 * The program takes three command-line arguments:
 * </p>
 * 
 * <ul>
 * <li>[base-path] base path of the ClueWeb09 distribution</li>
 * <li>[output-path] output path</li>
 * <li>[segment-num] segment number (1 through 10)</li>
 * <li>[docno-mapping-data-file] docno mapping data file</li>
 * </ul>
 * 
 * <p>
 * Here's a sample invocation:
 * </p>
 * 
 * <pre>
 * hadoop jar cloud9.jar edu.umd.cloud9.collection.clue.RepackClueWarcRecords \
 *  /umd/collections/ClueWeb09 /umd/collections/ClueWeb09.repacked.block/en.01 1 \
 *  /umd/collections/ClueWeb09.repacked.block/docno-mapping.dat block
 * </pre>
 * 
 * 
 * @author Jimmy Lin
 * 
 */
public class RepackClueWarcRecords extends Configured implements Tool {

	private static final Logger sLogger = Logger.getLogger(RepackClueWarcRecords.class);

	private static enum Records {
		TOTAL, PAGES
	};

	private static class MyMapper extends MapReduceBase implements
			Mapper<LongWritable, ClueWarcRecord, IntWritable, ClueWarcRecord> {

		private static final IntWritable sDocno = new IntWritable();
		private ClueWarcDocnoMapping mDocnoMapping = new ClueWarcDocnoMapping();

		public void configure(JobConf job) {
			try {
				mDocnoMapping.loadMapping(new Path(job.get("DocnoMappingDataFile")), FileSystem
						.get(job));
			} catch (Exception e) {
				throw new RuntimeException("Error loading docno mapping data file!");
			}
		}

		public void map(LongWritable key, ClueWarcRecord doc,
				OutputCollector<IntWritable, ClueWarcRecord> output, Reporter reporter)
				throws IOException {
			reporter.incrCounter(Records.TOTAL, 1);

			String id = doc.getHeaderMetadataItem("WARC-TREC-ID");

			if (id != null) {
				reporter.incrCounter(Records.PAGES, 1);

				sDocno.set(mDocnoMapping.getDocno(id));
				output.collect(sDocno, doc);
			}
		}
	}

	/**
	 * Creates an instance of this tool.
	 */
	public RepackClueWarcRecords() {
	}

	private static int printUsage() {
		System.out
				.println("usage: [base-path] [output-path] [segment-num] [docno-mapping-data-file] (block|record|none)");
		ToolRunner.printGenericCommandUsage(System.out);
		return -1;
	}

	/**
	 * Runs this tool.
	 */
	public int run(String[] args) throws Exception {
		if (args.length != 5) {
			printUsage();
			return -1;
		}

		String basePath = args[0];
		String outputPath = args[1];
		int segment = Integer.parseInt(args[2]);
		String data = args[3];
		String compressionType = args[4];

		if (!compressionType.equals("block") && !compressionType.equals("record")
				&& !compressionType.equals("none")) {
			System.err.println("Error: \"" + compressionType + "\" unknown compression type!");
			System.exit(-1);
		}

		// this is the default block size
		int blocksize = 1000000;

		JobConf conf = new JobConf(RepackClueWarcRecords.class);
		conf.setJobName("RepackClueWarcRecords:segment" + segment);

		conf.set("DocnoMappingDataFile", data);

		sLogger.info("Tool name: RepackClueWarcRecords");
		sLogger.info(" - base path: " + basePath);
		sLogger.info(" - output path: " + outputPath);
		sLogger.info(" - segement number: " + segment);
		sLogger.info(" - docno mapping data file: " + data);
		sLogger.info(" - compression type: " + compressionType);

		if (compressionType.equals("block")) {
			sLogger.info(" - block size: " + blocksize);
		}

		int mapTasks = 10;

		conf.setNumMapTasks(mapTasks);
		conf.setNumReduceTasks(0);

		ClueCollectionPathConstants.addEnglishCollectionPart(conf, basePath, segment);

		SequenceFileOutputFormat.setOutputPath(conf, new Path(outputPath));

		if (compressionType.equals("none")) {
			SequenceFileOutputFormat.setCompressOutput(conf, true);
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

		conf.setInputFormat(ClueWarcInputFormat.class);
		conf.setOutputFormat(SequenceFileOutputFormat.class);
		conf.setOutputKeyClass(IntWritable.class);
		conf.setOutputValueClass(ClueWarcRecord.class);

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
		int res = ToolRunner.run(new Configuration(), new RepackClueWarcRecords(), args);
		System.exit(res);
	}

}
