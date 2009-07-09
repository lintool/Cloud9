package edu.umd.cloud9.collection.clue;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

/**
 * <p>
 * Simple demo program to count the number of records in the ClueWeb09
 * collection, from the original distribution WARC files.
 * </p>
 * 
 * <p>
 * The program takes two command-line arguments:
 * </p>
 * 
 * <ul>
 * <li>[base-path] base path of the ClueWeb09 distribution</li>
 * <li>[segment-num] segment number (1 through 10)</li>
 * </ul>
 * 
 * <p>
 * Here's a sample invocation:
 * </p>
 * 
 * <pre>
 * hadoop jar cloud9.jar edu.umd.cloud9.collection.clue.DemoCountClueWarcRecords /umd/collections/ClueWeb09 1
 * </pre>
 * 
 * @author Jimmy Lin
 * 
 */
public class DemoCountClueWarcRecords extends Configured implements Tool {

	private static final Logger sLogger = Logger.getLogger(DemoCountClueWarcRecords.class);

	private static enum Records {
		TOTAL, PAGES
	};

	private static class MyMapper extends MapReduceBase implements
			Mapper<LongWritable, ClueWarcRecord, LongWritable, Text> {
		public void map(LongWritable key, ClueWarcRecord doc,
				OutputCollector<LongWritable, Text> output, Reporter reporter) throws IOException {
			reporter.incrCounter(Records.TOTAL, 1);

			String id = doc.getHeaderMetadataItem("WARC-TREC-ID");

			if (id != null)
				reporter.incrCounter(Records.PAGES, 1);
		}
	}

	/**
	 * Creates an instance of this tool.
	 */
	public DemoCountClueWarcRecords() {
	}

	private static int printUsage() {
		System.out.println("usage: [base-path] [segment-num]");
		ToolRunner.printGenericCommandUsage(System.out);
		return -1;
	}

	/**
	 * Runs this tool.
	 */
	public int run(String[] args) throws Exception {
		if (args.length != 2) {
			printUsage();
			return -1;
		}

		String basePath = args[0];
		int segment = Integer.parseInt(args[1]);

		sLogger.info("Tool name: DemoCountClueWarcRecords");
		sLogger.info(" - Base path: " + basePath);
		sLogger.info(" - Segement number: " + segment);

		int mapTasks = 10;

		long r = System.currentTimeMillis();
		String outputPath = "/tmp/" + r;

		JobConf conf = new JobConf(DemoCountClueWarcRecords.class);
		conf.setJobName("DemoCountCountCluePages:segment" + segment);

		conf.setNumMapTasks(mapTasks);
		conf.setNumReduceTasks(0);

		ClueCollectionPathConstants.addEnglishCollectionPart(conf, basePath, segment);

		FileOutputFormat.setOutputPath(conf, new Path(outputPath));
		FileOutputFormat.setCompressOutput(conf, false);

		conf.setInputFormat(ClueWarcInputFormat.class);
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);

		conf.setMapperClass(MyMapper.class);

		// delete the output directory if it exists already
		FileSystem.get(conf).delete(new Path(outputPath), true);

		JobClient.runJob(conf);

		// clean up
		FileSystem.get(conf).delete(new Path(outputPath), true);

		return 0;
	}

	/**
	 * Dispatches command-line arguments to the tool via the
	 * <code>ToolRunner</code>.
	 */
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new DemoCountClueWarcRecords(), args);
		System.exit(res);
	}

}
