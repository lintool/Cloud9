/*
 * Cloud9: A MapReduce Library for Hadoop
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You may
 * obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0 
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package edu.umd.cloud9.demo;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

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

/**
 * <p>
 * Simple word count demo. Counts words in the Bible+Shakespeare sample
 * collection. Expected trace of MapReduce operation:
 * </p>
 * 
 * <pre>
 * Map input records=156215
 * Map output records=1734298
 * Map input bytes=9068074
 * Map output bytes=15919397
 * Combine input records=1734298
 * Combine output records=135372
 * Reduce input groups=41788
 * Reduce input records=135372
 * Reduce output records=41788
 * </pre>
 * 
 */
public class DemoWordCount {

	// mapper: emits (token, 1) for every word occurrence
	protected static class MyMapper extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, IntWritable> {

		// reuse objects to save overhead of object creation
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output,
				Reporter reporter) throws IOException {
			String line = ((Text) value).toString();
			StringTokenizer itr = new StringTokenizer(line);
			while (itr.hasMoreTokens()) {
				word.set(itr.nextToken());
				output.collect(word, one);
			}
		}
	}

	// reducer: sums up all the counts
	protected static class MyReducer extends MapReduceBase implements
			Reducer<Text, IntWritable, Text, IntWritable> {

		// reuse objects
		private final static IntWritable SumValue = new IntWritable();

		public void reduce(Text key, Iterator<IntWritable> values,
				OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
			// sum up values
			int sum = 0;
			while (values.hasNext()) {
				sum += values.next().get();
			}
			SumValue.set(sum);
			output.collect(key, SumValue);
		}
	}

	protected DemoWordCount() {
	}

	/**
	 * Runs the demo.
	 */
	public static void main(String[] args) throws IOException {
		String inputPath = "/shared/sample-input/bible+shakes.nopunc";
		String outputPath = "sample-counts";

		int mapTasks = 20;
		int reduceTasks = 1;

		JobConf conf = new JobConf(DemoWordCount.class);
		conf.setJobName("DemoWordCount");

		conf.setNumMapTasks(mapTasks);
		conf.setNumReduceTasks(reduceTasks);

		FileInputFormat.setInputPaths(conf, new Path(inputPath));
		FileOutputFormat.setOutputPath(conf, new Path(outputPath));

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);

		conf.setMapperClass(MyMapper.class);
		conf.setCombinerClass(MyReducer.class);
		conf.setReducerClass(MyReducer.class);

		// Delete the output directory if it exists already
		Path outputDir = new Path(outputPath);
		FileSystem.get(conf).delete(outputDir, true);

		JobClient.runJob(conf);
	}
}
