package edu.umd.cloud9.util;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.JobConf;

public class SequenceFileProcessor<K extends WritableComparable, V extends Writable> {

	private Path mPath;
	private JobConf conf;
	private KeyValueProcess<K, V> mProcessor;
	private SequenceFile.Reader mReader;
	private K mKey;
	private V mValue;

	public SequenceFileProcessor(String location, KeyValueProcess<K, V> p)
			throws IOException {

		mPath = new Path(location);
		conf = new JobConf();

		mProcessor = p;

	}

	public void run() throws IOException {
		if (!FileSystem.get(conf).isFile(mPath)) {
			for (Path p : FileSystem.get(conf).listPaths(new Path[]{mPath})) {
				System.out.println("Applying to " + p);
				applyToFile(p);
			}
		} else {
			applyToFile(mPath);
		}

	}

	@SuppressWarnings("unchecked")
	public void applyToFile(Path path) throws IOException {
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
