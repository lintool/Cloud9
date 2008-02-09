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
 * Harness for processing one or more {@link SequenceFile}s within a single
 * process. This class is useful when you want to iterate through all key-value
 * pairs in a SequenceFile outside the context of a MapReduce task (or where
 * writing the computation as a MapReduce would be overkill). One example usage
 * case is to sum up all the values in a SequenceFile &mdash; this may be useful
 * if you want to make sure probabilities sum to one. Here's the code fragment
 * that would accomplish this:
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
 * <p>
 * The static method takes a path and and a {@link KeyValueProcess}. This
 * example uses an anonymous inner class to make the code more concise; the
 * static method returns the <code>KeyValueProcess</code> so that you can
 * retrieve results from it. The path can either be a file or a directory; if it
 * is a directory, all files in that directory are processed.
 * </p>
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

	/**
	 * Processes one or more <code>SequenceFile</code>s. The
	 * {@link KeyValueProcess} is applied to every key-value pair in the file if
	 * <code>path</code> denotes a file, or all files in the directory if
	 * <code>path</code> denotes a directory.
	 * 
	 * @param <K1>
	 *            type of key
	 * @param <V1>
	 *            type of value
	 * @param path
	 *            either a file or a directory
	 * @param p
	 *            the KeyValueProcess to apply
	 * @return the KeyValueProcess applied
	 */
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
