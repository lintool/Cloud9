/*
 * Cloud9: A MapReduce Library for Hadoop
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You may
 * obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0 
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

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

/**
 * Hadoop <code>InputFormat</code> for processing the TREC collection.
 * 
 * @author Jimmy Lin
 */
public class TrecDocumentInputFormat extends IndexableFileInputFormat<LongWritable, TrecDocument> {

	/**
	 * Returns a <code>RecordReader</code> for this <code>InputFormat</code>.
	 */
	public RecordReader<LongWritable, TrecDocument> getRecordReader(InputSplit inputSplit,
			JobConf conf, Reporter reporter) throws IOException {
		return new TrecDocumentRecordReader((FileSplit) inputSplit, conf);
	}

	/**
	 * Hadoop <code>RecordReader</code> for reading TREC-formatted documents.
	 */
	public static class TrecDocumentRecordReader implements
			RecordReader<LongWritable, TrecDocument> {

		private XMLRecordReader mReader;
		private Text mText = new Text();
		private LongWritable mLong = new LongWritable();

		/**
		 * Creates a <code>TrecDocumentRecordReader</code>.
		 */
		public TrecDocumentRecordReader(FileSplit split, JobConf conf) throws IOException {
			conf.set(XMLInputFormat.START_TAG_KEY, TrecDocument.XML_START_TAG);
			conf.set(XMLInputFormat.END_TAG_KEY, TrecDocument.XML_END_TAG);

			mReader = new XMLRecordReader(split, conf);
		}

		/**
		 * Reads the next key-value pair.
		 */
		public boolean next(LongWritable key, TrecDocument value) throws IOException {
			if (mReader.next(mLong, mText) == false)
				return false;
			key.set(mLong.get());
			TrecDocument.readDocument(value, mText.toString());
			return true;
		}

		/**
		 * Creates an object for the key.
		 */
		public LongWritable createKey() {
			return new LongWritable();
		}

		/**
		 * Creates an object for the value.
		 */
		public TrecDocument createValue() {
			return new TrecDocument();
		}

		/**
		 * Returns the current position in the input.
		 */
		public long getPos() throws IOException {
			return mReader.getPos();
		}

		/**
		 * Closes this InputSplit.
		 */
		public void close() throws IOException {
			mReader.close();
		}

		/**
		 * Returns progress on how much input has been consumed.
		 */
		public float getProgress() throws IOException {
			return ((float) (mReader.getPos() - mReader.getStart()))
					/ ((float) (mReader.getEnd() - mReader.getStart()));
		}

	}
}
