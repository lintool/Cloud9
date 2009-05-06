package edu.umd.cloud9.collection.trec;

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

public class TrecDocumentInputFormat extends IndexableFileInputFormat<LongWritable, TrecDocument> {

	public void configure(JobConf conf) {
	}

	public RecordReader<LongWritable, TrecDocument> getRecordReader(InputSplit inputSplit,
			JobConf conf, Reporter reporter) throws IOException {
		return new TrecDocumentRecordReader((FileSplit) inputSplit, conf);
	}

	public static class TrecDocumentRecordReader implements
			RecordReader<LongWritable, TrecDocument> {

		private XMLRecordReader mReader;
		private Text mText = new Text();
		private LongWritable mLong = new LongWritable();

		public TrecDocumentRecordReader(FileSplit split, JobConf conf) throws IOException {
			conf.set(XMLInputFormat.START_TAG_KEY, TrecDocument.XML_START_TAG);
			conf.set(XMLInputFormat.END_TAG_KEY, TrecDocument.XML_END_TAG);

			mReader = new XMLRecordReader(split, conf);
		}

		public boolean next(LongWritable key, TrecDocument value) throws IOException {
			if (mReader.next(mLong, mText) == false)
				return false;
			key.set(mLong.get());
			TrecDocument.readDocument(value, mText.toString());
			return true;
		}

		public LongWritable createKey() {
			return new LongWritable();
		}

		public TrecDocument createValue() {
			return new TrecDocument();
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
