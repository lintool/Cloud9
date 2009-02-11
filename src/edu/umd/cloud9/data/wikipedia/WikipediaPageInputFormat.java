package edu.umd.cloud9.data.wikipedia;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

import edu.umd.cloud9.data.XMLInputFormat.XmlRecordReader;

public class WikipediaPageInputFormat extends FileInputFormat<LongWritable, WikipediaPage> {

	public void configure(JobConf jobConf) {
	}

	public RecordReader<LongWritable, WikipediaPage> getRecordReader(InputSplit inputSplit,
			JobConf jobConf, Reporter reporter) throws IOException {
		return new WikipediaPageRecordReader((FileSplit) inputSplit, jobConf);
	}

	public static class WikipediaPageRecordReader implements
			RecordReader<LongWritable, WikipediaPage> {

		private XmlRecordReader mReader;
		private Text mText = new Text();
		private LongWritable mLong = new LongWritable();

		public WikipediaPageRecordReader(FileSplit split, JobConf jobConf) throws IOException {
			mReader = new XmlRecordReader(split, jobConf);
		}

		public boolean next(LongWritable key, WikipediaPage value) throws IOException {
			if (mReader.next(mLong, mText) == false)
				return false;

			WikipediaPage.readPage(value, mText.toString());
			return true;
		}

		public LongWritable createKey() {
			return new LongWritable();
		}

		public WikipediaPage createValue() {
			return new WikipediaPage();
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
