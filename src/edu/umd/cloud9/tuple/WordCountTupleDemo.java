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
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.lib.IdentityReducer;

import edu.umd.cloud9.tuple.UnpackKeysDemo.MapClass;

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
		public synchronized void reduce(WritableComparable key,
				Iterator values, OutputCollector output, Reporter reporter)
				throws IOException {
			int sum = 0;
			while (values.hasNext()) {
				sum += ((IntWritable) values.next()).get();
			}

			output.collect(key, new IntWritable(sum));
		}
	}

	// unpacks the byte array representation of the keys back into something
	// human readable
	public static class UnpackKeysClass extends MapReduceBase implements Mapper {
		private Tuple tuple = KEY_SCHEMA.instantiate();
		private Text textkey = new Text();

		public void map(WritableComparable key, Writable value,
				OutputCollector output, Reporter reporter) throws IOException {

			Tuple.unpackInto(tuple, ((BytesWritable) key).get());
			textkey.set(tuple.toString());
			output.collect(textkey, value);
		}
	}

	public static void main(String[] args) throws IOException {
		String inPath = "sample-input/bible+shakes.nopunc";
		String output1Path = "sample-counts-packed";
		String output2Path = "sample-counts-unpacked";
		int numMapTasks = 20;
		int numReduceTasks = 20;
		boolean local = false;

		if (local) {
			numMapTasks = 5;
			numReduceTasks = 10;
		}

		// the first MR is to do the actual word counting
		JobConf conf1 = new JobConf(WordCountTupleDemo.class);
		conf1.setJobName("wordcount");

		conf1.setInputPath(new Path(inPath));
		conf1.setNumMapTasks(numMapTasks);
		conf1.setNumReduceTasks(numReduceTasks);

		conf1.setMapperClass(MapClass.class);
		conf1.setCombinerClass(ReduceClass.class);
		conf1.setReducerClass(ReduceClass.class);

		conf1.setOutputPath(new Path(output1Path));
		conf1.setOutputKeyClass(BytesWritable.class);
		conf1.setOutputValueClass(IntWritable.class);
		conf1.setOutputFormat(SequenceFileOutputFormat.class);

		JobClient.runJob(conf1);

		// the second MR is to convert byte representation of keys back into
		// something human readable
		JobConf conf2 = new JobConf(UnpackKeysDemo.class);
		conf2.setJobName("unpack");

		conf2.setInputPath(new Path(output1Path));
		conf2.setInputFormat(SequenceFileInputFormat.class);
		conf2.setNumMapTasks(numMapTasks);
		conf2.setNumReduceTasks(numReduceTasks);

		conf2.setMapperClass(UnpackKeysClass.class);
		conf2.setCombinerClass(IdentityReducer.class);
		conf2.setReducerClass(IdentityReducer.class);

		conf2.setOutputPath(new Path(output2Path));
		conf2.setOutputKeyClass(Text.class);
		conf2.setOutputValueClass(IntWritable.class);

		JobClient.runJob(conf2);
	}
}
