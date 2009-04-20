package edu.umd.cloud9.collection.pmc;

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

public class PmcArticleInputFormat extends IndexableFileInputFormat<LongWritable, PmcArticle> {

	public void configure(JobConf conf) {
	}

	public RecordReader<LongWritable, PmcArticle> getRecordReader(InputSplit inputSplit,
			JobConf conf, Reporter reporter) throws IOException {
		return new PmcArticleRecordReader((FileSplit) inputSplit, conf);
	}

	public static class PmcArticleRecordReader implements RecordReader<LongWritable, PmcArticle> {

		private XMLRecordReader mReader;
		private Text mText = new Text();
		private LongWritable mLong = new LongWritable();

		public PmcArticleRecordReader(FileSplit split, JobConf conf) throws IOException {
			conf.set(XMLInputFormat.START_TAG_KEY, PmcArticle.XML_START_TAG);
			conf.set(XMLInputFormat.END_TAG_KEY, PmcArticle.XML_END_TAG);

			mReader = new XMLRecordReader(split, conf);
		}

		public boolean next(LongWritable key, PmcArticle value) throws IOException {
			if (mReader.next(mLong, mText) == false)
				return false;
			key.set(mLong.get());
			PmcArticle.readArticle(value, mText.toString());
			return true;
		}

		public LongWritable createKey() {
			return new LongWritable();
		}

		public PmcArticle createValue() {
			return new PmcArticle();
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
