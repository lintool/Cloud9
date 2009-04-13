package edu.umd.cloud9.collection.spinn3r;

import java.io.IOException;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

import edu.umd.cloud9.collection.IndexableFileInputFormat;
import edu.umd.cloud9.collection.XMLInputFormat;
import edu.umd.cloud9.collection.XMLInputFormat.XMLRecordReader;

public class Spinn3rItemInputFormat extends IndexableFileInputFormat<LongWritable, Spinn3rItem> {

	public void configure(JobConf conf) {
	}

	public RecordReader<LongWritable, Spinn3rItem> getRecordReader(InputSplit inputSplit,
			JobConf conf, Reporter reporter) throws IOException {
		return new Spinn3rItemRecordReader((FileSplit) inputSplit, conf);
	}

	public static class Spinn3rItemRecordReader implements RecordReader<LongWritable, Spinn3rItem> {
		static private long sOffset = 1000000000000000000L;

		private XMLRecordReader mReader;
		private Text mText = new Text();
		private LongWritable mLong = new LongWritable();
		private int mFileOffset;

		public Spinn3rItemRecordReader(FileSplit split, JobConf conf) throws IOException {
			conf.set(XMLInputFormat.START_TAG_KEY, Spinn3rItem.XML_START_TAG);
			conf.set(XMLInputFormat.END_TAG_KEY, Spinn3rItem.XML_END_TAG);

			mReader = new XMLRecordReader(split, conf);

			// this is the current file
			Path p = split.getPath();

			// get its directory listing
			FileSystem fs = p.getFileSystem(conf);
			FileStatus[] stats = fs.listStatus(p.getParent());
			for (int i = 0; i < stats.length; i++) {
				FileStatus s = stats[i];
				// find its numeric position in the directory
				if (s.getPath().equals(p)) {
					mFileOffset = i;
				}
			}
		}

		public boolean next(LongWritable key, Spinn3rItem value) throws IOException {
			if (mReader.next(mLong, mText) == false)
				return false;

			Spinn3rItem.readItem(value, mText.toString());

			key.set(mFileOffset * sOffset + mLong.get());
			value.setDocid(key.toString());
			return true;
		}

		public LongWritable createKey() {
			return new LongWritable();
		}

		public Spinn3rItem createValue() {
			return new Spinn3rItem();
		}

		public long getPos() throws IOException {
			return mReader.getPos();
		}

		public void close() throws IOException {
			mReader.close();
		}

		public float getProgress() throws IOException {
			return ((float) (mReader.getPos() - mReader.getStart()))
					/ ((float) (mReader.getEnd() - mReader.getStart()));
		}
	}
}
