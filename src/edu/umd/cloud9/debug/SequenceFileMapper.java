package edu.umd.cloud9.debug;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class SequenceFileMapper<K1 extends WritableComparable, V1 extends Writable, K2 extends WritableComparable, V2 extends Writable> {

	public void run(String path, Mapper<K1, V1, K2, V2> mapper) {
		DummyCollector collector = new DummyCollector();

		try {
			JobConf config = new JobConf();

			FileSystem fileSys = FileSystem.get(config);
			SequenceFile.Reader reader = new SequenceFile.Reader(fileSys, new Path(path), config);

			K1 key = (K1) reader.getKeyClass().newInstance();
			V1 value = (V1) reader.getValueClass().newInstance();

			while (reader.next(key, value)) {
				mapper.map(key, value, collector, Reporter.NULL);
			}
			reader.close();

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private class DummyCollector implements OutputCollector<K2, V2> {

		public DummyCollector() {

		}

		public void collect(K2 key, V2 value) {

		}
	}

}
