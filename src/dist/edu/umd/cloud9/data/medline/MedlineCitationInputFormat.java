package edu.umd.cloud9.data.medline;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

import edu.umd.cloud9.data.IndexableFileInputFormat;
import edu.umd.cloud9.data.XMLInputFormat;
import edu.umd.cloud9.data.XMLInputFormat.XMLRecordReader;

public class MedlineCitationInputFormat extends IndexableFileInputFormat<LongWritable, MedlineCitation> {

	public void configure(JobConf conf) {
	}

	public RecordReader<LongWritable, MedlineCitation> getRecordReader(InputSplit inputSplit,
			JobConf conf, Reporter reporter) throws IOException {
		return new MedlineCitationRecordReader((FileSplit) inputSplit, conf);
	}

	public static class MedlineCitationRecordReader implements
			RecordReader<LongWritable, MedlineCitation> {

		private XMLRecordReader mReader;
		private Text mText = new Text();
		private LongWritable mLong = new LongWritable();

		public MedlineCitationRecordReader(FileSplit split, JobConf conf) throws IOException {
			conf.set(XMLInputFormat.START_TAG_KEY, MedlineCitation.XML_START_TAG);
			conf.set(XMLInputFormat.END_TAG_KEY, MedlineCitation.XML_END_TAG);

			mReader = new XMLRecordReader(split, conf);
		}

		public boolean next(LongWritable key, MedlineCitation value) throws IOException {
			if (mReader.next(mLong, mText) == false)
				return false;
			key.set(mLong.get());
			MedlineCitation.readPage(value, mText.toString());
			return true;
		}

		public LongWritable createKey() {
			return new LongWritable();
		}

		public MedlineCitation createValue() {
			return new MedlineCitation();
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
