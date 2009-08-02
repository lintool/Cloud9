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

package edu.umd.cloud9.collection.medline;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobConfigurable;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

import edu.umd.cloud9.collection.IndexableFileInputFormat;
import edu.umd.cloud9.collection.XMLInputFormat;
import edu.umd.cloud9.collection.XMLInputFormat.XMLRecordReader;

/**
 * Hadoop <code>InputFormat</code> for processing the MEDLINE citations in XML
 * format.
 * 
 * @author Jimmy Lin
 */
public class MedlineCitationInputFormat extends
		IndexableFileInputFormat<LongWritable, MedlineCitation> implements JobConfigurable {
	
	private CompressionCodecFactory compressionCodecs = null;

	/**
	 * Creates a <code>MedlineCitationInputFormat</code>.
	 */
	public MedlineCitationInputFormat() {
	}

	public void configure(JobConf conf) {
		compressionCodecs = new CompressionCodecFactory(conf);
	}

	protected boolean isSplitable(FileSystem fs, Path file) {
		return compressionCodecs.getCodec(file) == null;
	}

	/**
	 * Returns a <code>RecordReader</code> for this <code>InputFormat</code>.
	 */
	public RecordReader<LongWritable, MedlineCitation> getRecordReader(InputSplit inputSplit,
			JobConf conf, Reporter reporter) throws IOException {
		return new MedlineCitationRecordReader((FileSplit) inputSplit, conf);
	}

	/**
	 * Hadoop <code>RecordReader</code> for reading MEDLINE citations in XML
	 * format.
	 */
	public static class MedlineCitationRecordReader implements
			RecordReader<LongWritable, MedlineCitation> {

		private XMLRecordReader mReader;
		private Text mText = new Text();
		private LongWritable mLong = new LongWritable();

		/**
		 * Creates a <code>MedlineCitationRecordReader</code>.
		 */
		public MedlineCitationRecordReader(FileSplit split, JobConf conf) throws IOException {
			conf.set(XMLInputFormat.START_TAG_KEY, MedlineCitation.XML_START_TAG);
			conf.set(XMLInputFormat.END_TAG_KEY, MedlineCitation.XML_END_TAG);

			mReader = new XMLRecordReader(split, conf);
		}

		/**
		 * Reads the next key-value pair.
		 */
		public boolean next(LongWritable key, MedlineCitation value) throws IOException {
			if (mReader.next(mLong, mText) == false)
				return false;
			key.set(mLong.get());
			MedlineCitation.readCitation(value, mText.toString());
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
		public MedlineCitation createValue() {
			return new MedlineCitation();
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
