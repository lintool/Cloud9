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

public class SequenceFileMapper {

	public void run(
			String path,
			Class<? extends Mapper<? extends WritableComparable, ? extends Writable, ? extends WritableComparable, ? extends Writable>> theClass) {
		DummyCollector collector = new DummyCollector();

		Mapper mapper = null;

		try {
			mapper = theClass.newInstance();
		} catch (Exception e) {
			e.printStackTrace();
		}

		try {
			JobConf config = new JobConf();

			FileSystem fileSys = FileSystem.get(config);
			SequenceFile.Reader reader = new SequenceFile.Reader(fileSys,
					new Path(path), config);

			WritableComparable key = (WritableComparable) reader.getKeyClass()
					.newInstance();
			Writable value = (Writable) reader.getValueClass().newInstance();

			while (reader.next(key, value)) {
				mapper.map(key, value, collector, Reporter.NULL);
			}
			reader.close();

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private class DummyCollector implements
			OutputCollector<WritableComparable, Writable> {

		public DummyCollector() {

		}

		public void collect(WritableComparable key, Writable value) {

		}
	}

}
