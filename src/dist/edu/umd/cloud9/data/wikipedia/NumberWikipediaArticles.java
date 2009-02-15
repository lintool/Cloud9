package edu.umd.cloud9.data.wikipedia;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
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
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextOutputFormat;

import edu.umd.cloud9.util.FSLineReader;

public class NumberWikipediaArticles {

	protected static enum PageTypes {
		TOTAL, REDIRECT, DISAMBIGUATION, EMPTY, ARTICLE, STUB
	};

	private static class MyMapper extends MapReduceBase implements
			Mapper<LongWritable, WikipediaPage, Text, IntWritable> {

		private final static Text mText = new Text();
		private final static IntWritable mInt = new IntWritable(1);

		public void map(LongWritable key, WikipediaPage p,
				OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
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

				mText.set(p.getTitle());
				output.collect(mText, mInt);
			}
		}
	}

	private static class MyReducer extends MapReduceBase implements
			Reducer<Text, IntWritable, Text, IntWritable> {

		private final static IntWritable mCnt = new IntWritable(0);

		public void reduce(Text key, Iterator<IntWritable> values,
				OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
			output.collect(key, mCnt);
			mCnt.set(mCnt.get() + 1);
		}
	}

	private NumberWikipediaArticles() {
	}

	static public void writeArticleTitlesData(String input, String output) throws IOException {
		System.out.println("Writing article titles to " + output);
		FSLineReader reader = new FSLineReader(input);
		List<String> list = new ArrayList<String>();

		System.out.print("Reading " + input);
		int cnt = 0;
		Text line = new Text();
		while (reader.readLine(line) > 0) {
			String[] arr = line.toString().split("\\t");
			list.add(arr[0]);
			cnt++;
			if (cnt % 100000 == 0) {
				System.out.print(".");
			}
		}
		reader.close();
		System.out.print("Done!\n");

		System.out.print("Writing " + output);
		FSDataOutputStream out = FileSystem.get(new Configuration()).create(new Path(output), true);
		out.writeInt(list.size());
		for (int i = 0; i < list.size(); i++) {
			out.writeUTF(list.get(i));
			cnt++;
			if (cnt % 100000 == 0) {
				System.out.print(".");
			}
		}
		out.close();
		System.out.print("Done!\n");
	}

	static public String[] readArticleTitlesData(Path p, FileSystem fs) throws IOException {
		FSDataInputStream in = fs.open(p);
		int sz = in.readInt();

		String[] arr = new String[sz];
		for (int i = 0; i < sz; i++) {
			arr[i] = in.readUTF();
		}
		in.close();

		return arr;
	}

	/**
	 * Runs the demo.
	 */
	public static void main(String[] args) throws IOException {
		if (args.length != 4) {
			System.out.println("usage: [xml-dump] [output-dir] [output-file] [num-mappers]");
			System.exit(-1);

		}
		String inputPath = args[0];
		String outputPath = args[1];
		String outputFile = args[2];
		int mapTasks = Integer.parseInt(args[3]);

		System.out.println("input: " + inputPath);
		System.out.println("output: " + outputPath);
		System.out.println("number of mappers: " + mapTasks);

		JobConf conf = new JobConf(NumberWikipediaArticles.class);
		conf.setJobName("wikipedia-processing");

		conf.setNumMapTasks(mapTasks);
		conf.setNumReduceTasks(1);

		FileInputFormat.setInputPaths(conf, new Path(inputPath));
		FileOutputFormat.setOutputPath(conf, new Path(outputPath));
		FileOutputFormat.setCompressOutput(conf, false);

		conf.setInputFormat(WikipediaPageInputFormat.class);
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);
		conf.setOutputFormat(TextOutputFormat.class);

		conf.setMapperClass(MyMapper.class);
		conf.setReducerClass(MyReducer.class);

		// delete the output directory if it exists already
		FileSystem.get(conf).delete(new Path(outputPath), true);

		JobClient.runJob(conf);

		NumberWikipediaArticles.writeArticleTitlesData(outputPath + "/part-00000", outputFile);
	}
}
