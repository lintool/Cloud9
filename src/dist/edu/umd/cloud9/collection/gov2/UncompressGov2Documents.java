package edu.umd.cloud9.collection.gov2;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;

public class UncompressGov2Documents {
	private static enum Documents {
		Count
	};

	private static class MyMapper extends MapReduceBase implements
			Mapper<LongWritable, Gov2Document, LongWritable, Gov2Document> {
		public void map(LongWritable key, Gov2Document doc,
				OutputCollector<LongWritable, Gov2Document> output, Reporter reporter)
				throws IOException {
			reporter.incrCounter(Documents.Count, 1);

			System.out.println(doc.getDocid());
			//System.out.println("============" + doc.getDocid() + "============\n"
			//		+ doc.getContent());
			output.collect(key, doc);
		}
	}

	private UncompressGov2Documents() {
	}

	/**
	 * Runs the demo.
	 */
	public static void main(String[] args) throws IOException {
		int mapTasks = 10;

		String outputPath = "/umd/collections/gov2.uncompressed/";

		JobConf conf = new JobConf(UncompressGov2Documents.class);
		conf.setJobName("UncompressGov2Documents");

		conf.setNumMapTasks(mapTasks);
		conf.setNumReduceTasks(0);

		FileInputFormat.addInputPath(conf, new Path("/umd/collections/gov2"));
		FileOutputFormat.setOutputPath(conf, new Path(outputPath));
		FileOutputFormat.setCompressOutput(conf, false);

		conf.setInputFormat(Gov2InputFormat.class);
		conf.setOutputFormat(SequenceFileOutputFormat.class);
		conf.setOutputKeyClass(LongWritable.class);
		conf.setOutputValueClass(Gov2Document.class);

		conf.setMapperClass(MyMapper.class);

		// delete the output directory if it exists already
		FileSystem.get(conf).delete(new Path(outputPath), true);

		JobClient.runJob(conf);
	}

}
