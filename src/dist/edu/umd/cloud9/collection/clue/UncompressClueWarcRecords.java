package edu.umd.cloud9.collection.clue;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.FileOutputFormat;
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
 * WARC files into <code>SequenceFiles</code>.
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
 * </ul>
 * 
 * <p>
 * Here's a sample invocation:
 * </p>
 * 
 * <pre>
 * hadoop jar cloud9.jar edu.umd.cloud9.collection.clue.UncompressClueWarcRecords \
 *  /umd/collections/ClueWeb09 /umd/collection/clue.en.segment.01 1
 * </pre>
 * 
 * 
 * @author Jimmy Lin
 * 
 */
@Deprecated
public class UncompressClueWarcRecords extends Configured implements Tool {

	private static final Logger sLogger = Logger.getLogger(UncompressClueWarcRecords.class);

	private static enum Records {
		TOTAL, PAGES
	};

	private static class MyMapper extends MapReduceBase implements
			Mapper<LongWritable, ClueWarcRecord, LongWritable, ClueWarcRecord> {
		public void map(LongWritable key, ClueWarcRecord doc,
				OutputCollector<LongWritable, ClueWarcRecord> output, Reporter reporter)
				throws IOException {
			reporter.incrCounter(Records.TOTAL, 1);

			String id = doc.getHeaderMetadataItem("WARC-TREC-ID");

			if (id != null) {
				reporter.incrCounter(Records.PAGES, 1);
				output.collect(key, doc);
			}
		}
	}

	/**
	 * Creates an instance of this tool.
	 */
	public UncompressClueWarcRecords() {
	}

	private static int printUsage() {
		System.out.println("usage: [base-path] [output-path] [segment-num]");
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
		int segment = Integer.parseInt(args[2]);

		sLogger.info("Tool name: DemoCountClueWarcRecords");
		sLogger.info(" - Base path: " + basePath);
		sLogger.info(" - Output path: " + outputPath);
		sLogger.info(" - Segement number: " + segment);

		int mapTasks = 10;

		JobConf conf = new JobConf(UncompressClueWarcRecords.class);
		conf.setJobName("UncompressClueRecords:segment" + segment);

		conf.setNumMapTasks(mapTasks);
		conf.setNumReduceTasks(0);

		ClueCollectionPathConstants.addEnglishCollectionPart(conf, basePath, segment);

		FileOutputFormat.setOutputPath(conf, new Path(outputPath));
		FileOutputFormat.setCompressOutput(conf, false);

		conf.setInputFormat(ClueWarcInputFormat.class);
		conf.setOutputFormat(SequenceFileOutputFormat.class);
		conf.setOutputKeyClass(LongWritable.class);
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
		int res = ToolRunner.run(new Configuration(), new UncompressClueWarcRecords(), args);
		System.exit(res);
	}

}
