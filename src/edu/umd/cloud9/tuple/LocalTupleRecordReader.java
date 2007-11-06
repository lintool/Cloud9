package edu.umd.cloud9.tuple;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapred.JobConf;

public class LocalTupleRecordReader {
	private LongWritable mKey = new LongWritable();
	private BytesWritable value = new BytesWritable();
	private SequenceFile.Reader mReader;

	private long cnt = 0;

	public LocalTupleRecordReader(String file) throws IOException {
		JobConf config = new JobConf();

		mReader = new SequenceFile.Reader(FileSystem.get(config),
				new Path(file), config);

	}

	public boolean read(Tuple tuple) throws IOException {
		if (mReader.next(mKey, value) == true) {
			//Tuple.unpackInto(tuple, value.get());
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
