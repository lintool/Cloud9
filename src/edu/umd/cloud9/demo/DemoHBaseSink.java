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

public class DemoHBaseSink {

	private static class ReduceClass extends TableReduce<LongWritable, Text> {

		public void reduce(LongWritable key, Iterator<Text> values,
				OutputCollector<Text, MapWritable> output, Reporter reporter)
				throws IOException {

			ImmutableBytesWritable bytes = new ImmutableBytesWritable(values
					.next().getBytes());
			MapWritable map = new MapWritable();
			Text col = new Text("default:text");

			map.put(col, bytes);

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

		conf.setNumMapTasks(mapTasks);
		conf.setNumReduceTasks(reduceTasks);

		conf.setInputPath(new Path(filename));

		conf.setMapperClass(IdentityMapper.class);
		conf.setCombinerClass(IdentityReducer.class);
		conf.setReducerClass(ReduceClass.class);

		TableReduce.initJob("test", ReduceClass.class, conf);

		JobClient.runJob(conf);
	}
}
