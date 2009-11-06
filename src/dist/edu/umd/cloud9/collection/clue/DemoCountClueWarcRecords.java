package edu.umd.cloud9.collection.clue;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

/**
 * <p>
 * Simple demo program to count the number of records in the ClueWeb09
 * collection, from either the original source WARC files or repacked
 * SequenceFiles (controlled by the first command-line parameter). The program
 * also verifies the docid to docno mappings.
 * </p>
 * 
 * <p>
 * The program takes four command-line arguments:
 * </p>
 * 
 * <ul>
 * <li>[original|repacked]: which version? 'original' for original source WARC
 * files; 'repacked' for SequenceFiles</li>
 * <li>[base-path]: base path of the ClueWeb09 distribution or base path of the
 * SequenceFiles</li>
 * <li>[segment-num]: segment number (1 through 10)</li>
 * <li>[mapping-file]: docno mapping data file</li>
 * </ul>
 * 
 * <p>
 * Here's a sample invocation:
 * </p>
 * 
 * <pre>
 * hadoop jar cloud9.jar edu.umd.cloud9.collection.clue.DemoCountSourceClueWarcRecords \
 *   original /umd/collections/ClueWeb09 1 /umd/collections/ClueWeb09/docno-mapping.dat
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
			Mapper<Writable, ClueWarcRecord, Writable, Text> {

		ClueWarcDocnoMapping mDocMapping = new ClueWarcDocnoMapping();

		public void configure(JobConf job) {
			try {
				Path[] localFiles = DistributedCache.getLocalCacheFiles(job);
				mDocMapping.loadMapping(localFiles[0], FileSystem.getLocal(job));
			} catch (Exception e) {
				e.printStackTrace();
				throw new RuntimeException("Error initializing DocnoMapping!");
			}
		}

		public void map(Writable key, ClueWarcRecord doc, OutputCollector<Writable, Text> output,
				Reporter reporter) throws IOException {
			reporter.incrCounter(Records.TOTAL, 1);

			String docid = doc.getHeaderMetadataItem("WARC-TREC-ID");
			int docno = mDocMapping.getDocno(docid);

			if (docid != null && docno != -1)
				reporter.incrCounter(Records.PAGES, 1);
		}
	}

	/**
	 * Creates an instance of this tool.
	 */
	public DemoCountClueWarcRecords() {
	}

	private static int printUsage() {
		System.out.println("usage: [original|repacked] [base-path] [segment-num] [mapping-file]");
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

		boolean repacked = true;
		if (args[0].equals("original")) {
			repacked = false;
		} else if (args[0].equals("repacked")) {
			repacked = true;
		} else {
			System.err.println("Expecting either 'original' or 'repacked' as first argument.");
			System.err.println("  'original' = original source WARC files");
			System.err.println("  'repacked' = repacked SequenceFiles");
			System.exit(-1);
		}

		String basePath = args[1];
		int segment = Integer.parseInt(args[2]);
		String mappingFile = args[3];

		sLogger.info("Tool name: DemoCountClueWarcRecords");
		sLogger.info(" - version: " + args[0]);
		sLogger.info(" - base path: " + basePath);
		sLogger.info(" - segment number: " + segment);
		sLogger.info(" - mapping file: " + mappingFile);

		int mapTasks = 10;

		long r = System.currentTimeMillis();
		String outputPath = "/tmp/" + r;

		JobConf conf = new JobConf(DemoCountClueWarcRecords.class);
		conf.setJobName("DemoCountClueWarcRecords:segment" + segment);

		conf.setNumMapTasks(mapTasks);
		conf.setNumReduceTasks(0);

		if (repacked) {
			FileInputFormat.addInputPath(conf, new Path(basePath));
		} else {
			ClueCollectionPathConstants.addEnglishCollectionPart(conf, basePath, segment);
		}

		DistributedCache.addCacheFile(new URI(mappingFile), conf);

		FileOutputFormat.setOutputPath(conf, new Path(outputPath));
		FileOutputFormat.setCompressOutput(conf, false);

		if (repacked) {
			conf.setInputFormat(SequenceFileInputFormat.class);
		} else {
			conf.setInputFormat(ClueWarcInputFormat.class);
		}

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
