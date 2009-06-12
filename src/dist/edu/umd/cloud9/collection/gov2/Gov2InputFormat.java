package edu.umd.cloud9.collection.gov2;

import java.io.DataInputStream;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

public class Gov2InputFormat extends FileInputFormat<LongWritable, Gov2Document> {

	/**
	 * Don't allow the files to be split!
	 */
	@Override
	protected boolean isSplitable(FileSystem fs, Path filename) {
		// ensure the input files are not splittable!
		return false;
	}

	/**
	 * Just return the record reader
	 */
	public RecordReader<LongWritable, Gov2Document> getRecordReader(InputSplit split, JobConf conf,
			Reporter reporter) throws IOException {
		return new Gov2DocumentRecordReader(conf, (FileSplit) split);
	}

	public static class Gov2DocumentRecordReader implements
			RecordReader<LongWritable, Gov2Document> {
		private long mRecordCount = 1;
		private Path mFilePath = null;
		private DataInputStream mCompressedInput = null;

		private long totalNumBytesRead = 0;

		public Gov2DocumentRecordReader(Configuration conf, FileSplit split) throws IOException {
			FileSystem fs = FileSystem.get(conf);
			mFilePath = split.getPath();

			CompressionCodec compressionCodec = new GzipCodec();
			mCompressedInput = new DataInputStream(compressionCodec.createInputStream(fs
					.open(mFilePath)));
		}

		public boolean next(LongWritable key, Gov2Document doc) throws IOException {
			DataInputStream whichStream = mCompressedInput;

			boolean status = Gov2Document.readNextGov2Document(doc, whichStream);
			
			if ( !status ) return false;

			// totalNumBytesRead += (long) newRecord.getTotalRecordLength();
			// newRecord.setWarcFilePath(mFilePath.toString());

			key.set(mRecordCount);

			mRecordCount++;
			return true;
		}

		public LongWritable createKey() {
			return new LongWritable();
		}

		public Gov2Document createValue() {
			return new Gov2Document();
		}

		public long getPos() throws IOException {
			return totalNumBytesRead;
		}

		public void close() throws IOException {
			mCompressedInput.close();
		}

		public float getProgress() throws IOException {
			return (float) mRecordCount / 40000f;
		}
	}
}
