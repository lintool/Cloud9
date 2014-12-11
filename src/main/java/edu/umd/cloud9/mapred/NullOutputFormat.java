package edu.umd.cloud9.mapred;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Progressable;

@Deprecated
public class NullOutputFormat implements OutputFormat<NullWritable, NullWritable> {

	public void checkOutputSpecs(FileSystem ignored, JobConf job) {

	}

	public RecordWriter<NullWritable, NullWritable> getRecordWriter(FileSystem ignored,
			JobConf job, String name, Progressable progress) {
		return new NullRecordWriter();
	}

	public static class NullRecordWriter implements RecordWriter<NullWritable, NullWritable> {

		public void close(Reporter reporter) {

		}

		public void write(NullWritable key, NullWritable value) {

		}
	}
}
