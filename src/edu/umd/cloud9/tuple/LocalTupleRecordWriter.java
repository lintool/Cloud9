package edu.umd.cloud9.tuple;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapred.JobConf;

public class LocalTupleRecordWriter {

	private BytesWritable mBytes = new BytesWritable();
	private LongWritable mLong = new LongWritable();

	private long mCnt = 0;

	private SequenceFile.Writer writer;

	public LocalTupleRecordWriter(String file) throws IOException {
		JobConf config = new JobConf();

		writer = SequenceFile.createWriter(FileSystem.get(config), config,
				new Path(file), LongWritable.class, BytesWritable.class);
	}

	public void add(Tuple tuple) throws IOException {
		/*byte[] buf = tuple.pack();

		mLong.set(mCnt);
		mBytes.set(buf, 0, buf.length);
		writer.append(mLong, mBytes);
		mCnt++;*/
	}

	public long getRecordCount() {
		return mCnt;
	}

	public void close() throws IOException {
		writer.close();
	}
}
