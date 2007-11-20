package edu.umd.cloud9.util;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapred.JobConf;

import edu.umd.cloud9.tuple.Tuple;

public class LocalTupleRecordReader {
	private LongWritable mKey = new LongWritable();
	private SequenceFile.Reader mReader;

	private long cnt = 0;

	public LocalTupleRecordReader(String file) throws IOException {
		JobConf config = new JobConf();

		mReader = new SequenceFile.Reader(FileSystem.get(config),
				new Path(file), config);
	}

	public boolean read(Tuple tuple) throws IOException {
		if (mReader.next(mKey, tuple) == true) {
			cnt++;
		} else {
			return false;
		}

		return true;
	}

	public long getRecordCount() {
		return cnt;
	}

	public void close() throws IOException {
		mReader.close();
	}

}
