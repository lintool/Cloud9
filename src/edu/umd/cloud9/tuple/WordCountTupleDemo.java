package edu.umd.cloud9.tuple;

import java.io.ByteArrayOutputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
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
import org.apache.hadoop.mapred.SequenceFileOutputFormat;

public class WordCountTupleDemo {

	public static final Schema KEY_SCHEMA = new Schema();
	static {
		KEY_SCHEMA.addField("Token", String.class, "");
		KEY_SCHEMA.addField("OddOrEven", Integer.class, new Integer(1));
	}
	
	// similar to WordCount, except that it keeps track of counts in lines that
	// are odd # chars in length or even # of chars in length --- only point is
	// to demo how tuples might be used
	public static class MapClass extends MapReduceBase implements Mapper {
		private final static IntWritable one = new IntWritable(1);
		
		private BytesWritable bytekey = new BytesWritable();
		private Tuple tuple = KEY_SCHEMA.instantiate();
		
		public void map(WritableComparable key, Writable value,
				OutputCollector output, Reporter reporter) throws IOException {
			String line = ((Text) value).toString();
			StringTokenizer itr = new StringTokenizer(line);
			while (itr.hasMoreTokens()) {
				String token = itr.nextToken();

				tuple.set(0, token);
				tuple.set(1, line.length() % 2);
				byte[] bytes = tuple.pack();
				
				bytekey.set(bytes, 0, bytes.length);
		
				output.collect(bytekey, one);
			}
		}
	}

	public static class ReduceClass extends MapReduceBase implements Reducer {
		public synchronized void reduce(WritableComparable key, Iterator values,
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

		JobConf conf = new JobConf(WordCountTupleDemo.class);
		conf.setJobName("wordcount");

		conf.setOutputKeyClass(BytesWritable.class);
		conf.setOutputValueClass(IntWritable.class);

		conf.setMapperClass(MapClass.class);
		conf.setCombinerClass(ReduceClass.class);
		conf.setReducerClass(ReduceClass.class);

		conf.setNumMapTasks(mapTasks);
		conf.setNumReduceTasks(reduceTasks);
		conf.setInputPath(new Path(filename));
		
		conf.setOutputFormat(SequenceFileOutputFormat.class);
		conf.setOutputPath(new Path(outputPath));

		JobClient.runJob(conf);
	}
}
