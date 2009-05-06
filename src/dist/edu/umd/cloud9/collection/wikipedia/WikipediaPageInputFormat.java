package edu.umd.cloud9.collection.wikipedia;

import java.io.IOException;

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

/**
 * Hadoop InputFormat for processing Wikipedia pages from the XML dumps.
 */
public class WikipediaPageInputFormat extends IndexableFileInputFormat<LongWritable, WikipediaPage> {

	public void configure(JobConf conf) {
	}

	public RecordReader<LongWritable, WikipediaPage> getRecordReader(InputSplit inputSplit,
			JobConf conf, Reporter reporter) throws IOException {
		return new WikipediaPageRecordReader((FileSplit) inputSplit, conf);
	}

	/**
	 * Hadoop RecordReader for reading Wikipedia pages from the XML dumps.
	 */
	public static class WikipediaPageRecordReader implements
			RecordReader<LongWritable, WikipediaPage> {

		private XMLRecordReader mReader;
		private Text mText = new Text();
		private LongWritable mLong = new LongWritable();

		public WikipediaPageRecordReader(FileSplit split, JobConf conf) throws IOException {
			conf.set(XMLInputFormat.START_TAG_KEY, WikipediaPage.XML_START_TAG);
			conf.set(XMLInputFormat.END_TAG_KEY, WikipediaPage.XML_END_TAG);

			mReader = new XMLRecordReader(split, conf);
		}

		public boolean next(LongWritable key, WikipediaPage value) throws IOException {
			if (mReader.next(mLong, mText) == false)
				return false;
			key.set(mLong.get());
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
