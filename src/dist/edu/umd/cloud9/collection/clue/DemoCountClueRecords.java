package edu.umd.cloud9.collection.clue;

import java.io.IOException;

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
import org.apache.log4j.Logger;

public class DemoCountClueRecords {
	private static final Logger sLogger = Logger.getLogger(DemoCountClueRecords.class);

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

			//sLogger.info(id + ", " + doc.getTotalRecordLength() + ", " + doc.getContent().length());
			sLogger.info(id + ", " + doc.getTotalRecordLength());
			
			//System.out.println("###BEGIN-" + doc.getHeaderMetadataItem("WARC-TREC-ID")+ "###" + doc.getContent() + "###END###");
		}
	}

	private DemoCountClueRecords() {
	}

	/**
	 * Runs the demo.
	 */
	public static void main(String[] args) throws IOException {
		int mapTasks = 10;

		long r = System.currentTimeMillis();
		String outputPath = "/tmp/" + r;

		JobConf conf = new JobConf(DemoCountClueRecords.class);
		conf.setJobName("DemoCountCountCluePages");

		conf.setNumMapTasks(mapTasks);
		conf.setNumReduceTasks(0);

		//FileInputFormat.addInputPath(conf, new Path("/umd/collections/crawldata/ClueWeb09_English_1/en0000/"));
		ClueCollectionPathConstants.addEnglishTinyPaths(conf, "/umd/collections/crawldata");
		
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
	}
}
