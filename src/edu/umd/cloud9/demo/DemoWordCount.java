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

/**
 * Simple word count demo. Counts words in the Bible+Shakespeare sample
 * collection.
 */
public class DemoWordCount {

	// mapper: emits (token, 1) for every word occurrence
	private static class MapClass extends MapReduceBase implements Mapper {

		// reuse objects to save overhead of object creation
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

	// reducer: sums up all the counts
	private static class ReduceClass extends MapReduceBase implements Reducer {

		// reuse objects
		private final static IntWritable SumValue = new IntWritable();

		public void reduce(WritableComparable key, Iterator values,
				OutputCollector output, Reporter reporter) throws IOException {
			// sum up values
			int sum = 0;
			while (values.hasNext()) {
				sum += ((IntWritable) values.next()).get();
			}
			SumValue.set(sum);
			output.collect(key, SumValue);
		}
	}

	private DemoWordCount() {
	}

	/**
	 * Runs the demo.
	 */
	public static void main(String[] args) throws IOException {
		String filename = "sample-input/bible+shakes.nopunc";
		String outputPath = "sample-counts";
		int mapTasks = 20;
		int reduceTasks = 1;

		JobConf conf = new JobConf(DemoWordCount.class);
		conf.setJobName("wordcount");

		conf.setNumMapTasks(mapTasks);
		conf.setNumReduceTasks(reduceTasks);

		conf.setInputPath(new Path(filename));
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);
		conf.setOutputPath(new Path(outputPath));

		conf.setMapperClass(MapClass.class);
		conf.setCombinerClass(ReduceClass.class);
		conf.setReducerClass(ReduceClass.class);

		JobClient.runJob(conf);
	}
}
