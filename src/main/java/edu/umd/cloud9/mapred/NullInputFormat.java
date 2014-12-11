package edu.umd.cloud9.mapred;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

// Note, there was a thread on the Hadoop users list on exactly this issue. 
// 5/8/2009, "How to write a map() method that needs no input?"
public class NullInputFormat implements InputFormat<NullWritable, NullWritable> {

	public RecordReader<NullWritable, NullWritable> getRecordReader(InputSplit split, JobConf job,
			Reporter reporter) {
		return new NullRecordReader();
	}

	public InputSplit[] getSplits(JobConf job, int numSplits) {
		InputSplit[] splits = new InputSplit[numSplits];

		for (int i = 0; i < numSplits; i++)
			splits[i] = new NullInputSplit();

		return splits;

	}

	public void validateInput(JobConf job) {
	}

	public static class NullRecordReader implements RecordReader<NullWritable, NullWritable> {

		private boolean returnRecord = true;

		public NullRecordReader() {

		}

		public boolean next(NullWritable key, NullWritable value) throws IOException {
			if (returnRecord == true) {
				returnRecord = false;
				return true;
			}

			return returnRecord;
		}

		public NullWritable createKey() {
			return NullWritable.get();
		}

		public NullWritable createValue() {
			return NullWritable.get();
		}

		public long getPos() throws IOException {
			return 0;
		}

		public float getProgress() throws IOException {
			return 0.0f;
		}

		public void close() {
		}
	}

}
