package edu.umd.cloud9.util;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.JobConf;

/**
 * <p>
 * Harness for processing {@link SequenceFile}s with in single process
 * sequentially. This class is useful when you need to iterate through all
 * key-value pairs in a SequenceFile outside the context of a MapReduce. One
 * example usage case is to sum up all the values in a SequenceFile &mdash; this
 * may be useful if you want to make sure probabilities sum to one. Here's the
 * code fragment that would accomplish this:
 * </p>
 * 
 * <pre>
 * KeyValueProcess&lt;Tuple, FloatWritable&gt; process = SequenceFileProcessor
 * 		.&lt;Tuple, FloatWritable&gt; process(&quot;foo&quot;,
 * 				new KeyValueProcess&lt;Tuple, FloatWritable&gt;() {
 * 					public float sum = 0.0f;
 * 
 * 					public void process(Tuple tuple, FloatWritable f) {
 * 						sum += f.get();
 * 					}
 * 
 * 					public void report() {
 * 						setProperty(&quot;sum&quot;, sum);
 * 					}
 * 				});
 * 
 * float sum = (Float) process.getProperty(&quot;sum&quot;);
 * </pre>
 * 
 * @param <K>
 *            type of key
 * @param <V>
 *            type of value
 */
public class SequenceFileProcessor<K extends WritableComparable, V extends Writable> {

	private Path mPath;
	private JobConf conf;
	private KeyValueProcess<K, V> mProcessor;
	private SequenceFile.Reader mReader;
	private K mKey;
	private V mValue;

	public static <K1 extends WritableComparable, V1 extends Writable> KeyValueProcess<K1, V1> process(
			String path, KeyValueProcess<K1, V1> p) {

		try {
			SequenceFileProcessor<K1, V1> processor = new SequenceFileProcessor<K1, V1>(
					path, p);
			processor.run();
		} catch (Exception e) {
			e.printStackTrace();
		}

		return p;
	}

	private SequenceFileProcessor(String location, KeyValueProcess<K, V> p)
			throws IOException {

		mPath = new Path(location);
		conf = new JobConf();

		mProcessor = p;

	}

	private void run() throws IOException {
		if (!FileSystem.get(conf).isFile(mPath)) {
			for (Path p : FileSystem.get(conf).listPaths(new Path[] { mPath })) {
				// System.out.println("Applying to " + p);
				applyToFile(p);
			}
		} else {
			applyToFile(mPath);
		}

	}

	@SuppressWarnings("unchecked")
	private void applyToFile(Path path) throws IOException {
		mReader = new SequenceFile.Reader(FileSystem.get(conf), path, conf);

		try {
			mKey = (K) mReader.getKeyClass().newInstance();
			mValue = (V) mReader.getValueClass().newInstance();
		} catch (Exception e) {
			e.printStackTrace();
		}

		while (mReader.next(mKey, mValue) == true) {
			mProcessor.process(mKey, mValue);
		}

		mReader.close();
		mProcessor.report();
	}
}
