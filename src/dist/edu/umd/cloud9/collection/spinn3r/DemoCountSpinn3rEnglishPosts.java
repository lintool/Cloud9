package edu.umd.cloud9.collection.spinn3r;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class DemoCountSpinn3rEnglishPosts {

	private static enum Languages {
		TOTAL, EN
	};

	private static class MyMapper extends MapReduceBase implements
			Mapper<LongWritable, Spinn3rItem, LongWritable, Text> {
		public void map(LongWritable key, Spinn3rItem item,
				OutputCollector<LongWritable, Text> output, Reporter reporter) throws IOException {

			if (item.getLanguage().equals("en")) {
				reporter.incrCounter(Languages.EN, 1);
			}
			reporter.incrCounter(Languages.TOTAL, 1);

		}
	}

	private DemoCountSpinn3rEnglishPosts() {
	}

	/**
	 * Runs the demo.
	 */
	public static void main(String[] args) throws IOException {
		String inputPath = "/umd/collections/spinn3r.raw/";
		int mapTasks = 200;

		System.out.println("input: " + inputPath);

		long r = System.currentTimeMillis();
		String outputPath = "/tmp/" + r;

		JobConf conf = new JobConf(DemoCountSpinn3rEnglishPosts.class);
		conf.setJobName("spinn3r-analysis");

		conf.setNumMapTasks(mapTasks);
		conf.setNumReduceTasks(0);

		FileInputFormat.setInputPaths(conf, new Path(inputPath));
		FileOutputFormat.setOutputPath(conf, new Path(outputPath));
		FileOutputFormat.setCompressOutput(conf, false);

		conf.setInputFormat(Spinn3rItemInputFormat.class);
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
