package edu.umd.cloud9.collection.clue;

import java.io.IOException;

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

public class UncompressClueRecords {

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

			// System.out.println(doc.getRecord().getHeaderMetadataItem("WARC-TREC-ID")
			// + "#"
			// + doc.getRecord().getTotalRecordLength());
		}
	}

	private UncompressClueRecords() {
	}

	/**
	 * Runs the demo.
	 */
	public static void main(String[] args) throws IOException {
		int mapTasks = 10;

		String outputPath = "/umd/collections/clue.en.small/";

		JobConf conf = new JobConf(UncompressClueRecords.class);
		conf.setJobName("UncompressClueRecords");

		conf.setNumMapTasks(mapTasks);
		conf.setNumReduceTasks(0);

		ClueCollectionPathConstants.addEnglishSmallCollection(conf, "/umd/collections/crawldata");

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
	}

}
