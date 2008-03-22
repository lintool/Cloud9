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

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapred.TableReduce;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.lib.IdentityMapper;
import org.apache.hadoop.mapred.lib.IdentityReducer;

/**
 * <p>
 * Demo that illustrates HBase as a data sink. This demo packs the sample
 * collection into HBase. Text is stored in the "default:text" column, with the
 * byte offset as the row key. See {@link DemoHBaseSource} for an illustration
 * of HBase as a data source.
 * </p>
 * 
 * @see DemoHBaseSource
 */
public class DemoHBaseSink {

	private static class ReduceClass extends TableReduce<LongWritable, Text> {

		// this is the column we're going to be writing
		private static final Text col = new Text("default:text");

		// this map holds the columns per row
		private MapWritable map = new MapWritable();

		public void reduce(LongWritable key, Iterator<Text> values,
				OutputCollector<Text, MapWritable> output, Reporter reporter)
				throws IOException {

			// contents must be ImmutableBytesWritable
			ImmutableBytesWritable bytes = new ImmutableBytesWritable(values
					.next().getBytes());

			// populate the current row
			map.clear();
			map.put(col, bytes);

			// add the row with the key as the row id
			output.collect(new Text(key.toString()), map);
		}
	}

	private DemoHBaseSink() {
	}

	/**
	 * Runs the demo.
	 */
	public static void main(String[] args) throws IOException {
		String filename = "/shared/sample";

		int mapTasks = 1;
		int reduceTasks = 1;

		JobConf conf = new JobConf(DemoHBaseSink.class);
		conf.setJobName("wordcount");

		// must initialize the TableReduce before running job
		TableReduce.initJob("test", ReduceClass.class, conf);

		conf.setNumMapTasks(mapTasks);
		conf.setNumReduceTasks(reduceTasks);

		conf.setInputPath(new Path(filename));

		conf.setMapperClass(IdentityMapper.class);
		conf.setCombinerClass(IdentityReducer.class);
		conf.setReducerClass(ReduceClass.class);

		JobClient.runJob(conf);
	}
}
