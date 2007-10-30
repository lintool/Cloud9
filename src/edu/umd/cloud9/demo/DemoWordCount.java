package edu.umd.cloud9.demo;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class DemoWordCount {

	public static class MapClass extends MapReduceBase implements Mapper {
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		public void map(WritableComparable key, Writable value,
				OutputCollector output, Reporter reporter) throws IOException {
			String line = ((Text) value).toString();
			StringTokenizer itr = new StringTokenizer(line);
			while (itr.hasMoreTokens()) {
				word.set(itr.nextToken());
				output.collect(word, one);
			}
		}
	}

	public static class ReduceClass extends MapReduceBase implements Reducer {
		public void reduce(WritableComparable key, Iterator values,
				OutputCollector output, Reporter reporter) throws IOException {
			int sum = 0;
			while (values.hasNext()) {
				sum += ((IntWritable) values.next()).get();
			}
			output.collect(key, new IntWritable(sum));
		}
	}

	public static void main(String[] args) throws IOException {
		String filename = "sample-input/bible+shakes.nopunc";
		String outputPath = "sample-counts";
		int mapTasks = 20;
		int reduceTasks = 20;
		boolean local = false;

		if (local) {
			mapTasks = 5;
			reduceTasks = 10;
		}

		JobConf conf = new JobConf(DemoWordCount.class);
		conf.setJobName("wordcount");

		// the keys are words (strings)
		conf.setOutputKeyClass(Text.class);
		// the values are counts (ints)
		conf.setOutputValueClass(IntWritable.class);

		conf.setMapperClass(MapClass.class);
		conf.setCombinerClass(ReduceClass.class);
		conf.setReducerClass(ReduceClass.class);

		conf.setNumMapTasks(mapTasks);
		conf.setNumReduceTasks(reduceTasks);
		conf.setInputPath(new Path(filename));
		conf.setOutputPath(new Path(outputPath));

		JobClient.runJob(conf);
	}
}
