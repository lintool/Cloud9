package edu.umd.cloud9.data.wikipedia;

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

import edu.umd.cloud9.data.XMLInputFormat;

public class DemoWikipediaHadoop {

	protected static enum PageTypes {
		TOTAL, REDIRECT, DISAMBIGUATION, EMPTY, ARTICLE, STUB
	};

	public static class MyMapper extends MapReduceBase implements
			Mapper<LongWritable, WikipediaPage, LongWritable, Text> {

		public void map(LongWritable key, WikipediaPage p,
				OutputCollector<LongWritable, Text> output, Reporter reporter) throws IOException {
			reporter.incrCounter(PageTypes.TOTAL, 1);

			if (p.isRedirect()) {
				reporter.incrCounter(PageTypes.REDIRECT, 1);

			} else if (p.isDisambiguation()) {
				reporter.incrCounter(PageTypes.DISAMBIGUATION, 1);
			} else if (p.isEmpty()) {
				reporter.incrCounter(PageTypes.EMPTY, 1);
			} else {
				reporter.incrCounter(PageTypes.ARTICLE, 1);

				if (p.isStub()) {
					reporter.incrCounter(PageTypes.STUB, 1);
				}
			}
		}
	}

	protected DemoWikipediaHadoop() {
	}

	/**
	 * Runs the demo.
	 */
	public static void main(String[] args) throws IOException {
		if (args.length != 2) {
			System.out.println("usage: [xml-dump] [num-mappers]");
			System.exit(-1);

		}
		String inputPath = args[0];
		int mapTasks = Integer.parseInt(args[1]);

		System.out.println("input: " + inputPath);
		System.out.println("number of mappers: " + mapTasks);

		long r = System.currentTimeMillis();
		String outputPath = "/tmp/" + r;

		JobConf conf = new JobConf(DemoWikipediaHadoop.class);
		conf.setJobName("wikipedia-demo");

		conf.setNumMapTasks(mapTasks);
		conf.setNumReduceTasks(0);

		FileInputFormat.setInputPaths(conf, new Path(inputPath));
		FileOutputFormat.setOutputPath(conf, new Path(outputPath));
		FileOutputFormat.setCompressOutput(conf, false);

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);

		conf.setInputFormat(WikipediaPageInputFormat.class);
		conf.set(XMLInputFormat.START_TAG_KEY, WikipediaPage.XML_START_TAG);
		conf.set(XMLInputFormat.END_TAG_KEY, WikipediaPage.XML_END_TAG);

		conf.setMapperClass(MyMapper.class);

		// delete the output directory if it exists already
		FileSystem.get(conf).delete(new Path(outputPath), true);

		JobClient.runJob(conf);

		// clean up
		FileSystem.get(conf).delete(new Path(outputPath), true);
	}
}
