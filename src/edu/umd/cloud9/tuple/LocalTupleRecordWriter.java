package edu.umd.cloud9.tuple;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapred.JobConf;

public class LocalTupleRecordWriter {

	private LongWritable mLong = new LongWritable();

	private long mCnt = 0;

	private SequenceFile.Writer writer;

	public LocalTupleRecordWriter(String file) throws IOException {
		JobConf config = new JobConf();

		writer = SequenceFile.createWriter(FileSystem.get(config), config,
				new Path(file), LongWritable.class, Tuple.class);
	}

	public void add(Tuple tuple) throws IOException {
		mLong.set(mCnt);
		writer.append(mLong, tuple);
		mCnt++;
	}

	public long getRecordCount() {
		return mCnt;
	}

	public void close() throws IOException {
		writer.close();
	}
}
