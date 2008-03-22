package edu.umd.cloud9.demo;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HStoreKey;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapred.TableInputFormat;
import org.apache.hadoop.hbase.mapred.TableMap;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class DemoHBaseSource {

	// mapper: emits (token, 1) for every word occurrence
	private static class MapClass extends TableMap<Text, IntWritable> {

		// reuse objects to save overhead of object creation
		private final static IntWritable one = new IntWritable(1);
		private final static Text textcol = new Text("default:text");
		private Text word = new Text();

		public void map(HStoreKey key, MapWritable cols,
				OutputCollector<Text, IntWritable> output, Reporter reporter)
				throws IOException {
			
			String line = Text.decode(((ImmutableBytesWritable) cols.get(textcol)).get());
			
			StringTokenizer itr = new StringTokenizer(line);
			while (itr.hasMoreTokens()) {
				word.set(itr.nextToken());
				output.collect(word, one);
			}
		}
	}

	// reducer: sums up all the counts
	private static class ReduceClass extends MapReduceBase implements
			Reducer<Text, IntWritable, Text, IntWritable> {

		// reuse objects
		private final static IntWritable SumValue = new IntWritable();

		public void reduce(Text key, Iterator<IntWritable> values,
				OutputCollector<Text, IntWritable> output, Reporter reporter)
				throws IOException {
			// sum up values
			int sum = 0;
			while (values.hasNext()) {
				sum += values.next().get();
			}
			SumValue.set(sum);
			output.collect(key, SumValue);
		}
	}

	private DemoHBaseSource() {
	}

	/**
	 * Runs the demo.
	 */
	public static void main(String[] args) throws IOException {
		String outputPath = "sample-counts2";
		
		int mapTasks = 1;
		int reduceTasks = 1;

		JobConf conf = new JobConf(DemoHBaseSource.class);
		
		TableMap.initJob("test", "default:text", MapClass.class, conf);

		conf.setJobName("wordcount");

		conf.setNumMapTasks(mapTasks);
		conf.setNumReduceTasks(reduceTasks);

		conf.setInputFormat(TableInputFormat.class);
		
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);
		conf.setOutputPath(new Path(outputPath));

		conf.setMapperClass(MapClass.class);
		conf.setCombinerClass(ReduceClass.class);
		conf.setReducerClass(ReduceClass.class);

		JobClient.runJob(conf);
	}
}
