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
 * Hadoop <code>InputFormat</code> for processing Wikipedia pages from the XML
 * dumps.
 * 
 * @author Jimmy Lin
 */
@SuppressWarnings("deprecation")
public class WikipediaPageInputFormat extends IndexableFileInputFormat<LongWritable, WikipediaPage> {

	/**
	 * Returns a <code>RecordReader</code> for this <code>InputFormat</code>.
	 */
	public RecordReader<LongWritable, WikipediaPage> getRecordReader(InputSplit inputSplit,
			JobConf conf, Reporter reporter) throws IOException {
		return new WikipediaPageRecordReader((FileSplit) inputSplit, conf);
	}

	/**
	 * Hadoop <code>RecordReader</code> for reading Wikipedia pages from the
	 * XML dumps.
	 */
	public static class WikipediaPageRecordReader implements RecordReader<LongWritable, WikipediaPage> {
		private XMLRecordReader reader;
		private Text text = new Text();
		private LongWritable offset = new LongWritable();

		/**
		 * Creates a <code>WikipediaPageRecordReader</code>.
		 */
		public WikipediaPageRecordReader(FileSplit split, JobConf conf) throws IOException {
			conf.set(XMLInputFormat.START_TAG_KEY, WikipediaPage.XML_START_TAG);
			conf.set(XMLInputFormat.END_TAG_KEY, WikipediaPage.XML_END_TAG);

			reader = new XMLRecordReader(split, conf);
		}

		/**
		 * Reads the next key-value pair.
		 */
		public boolean next(LongWritable key, WikipediaPage value) throws IOException {
			if (reader.next(offset, text) == false)
				return false;
			key.set(offset.get());
			WikipediaPage.readPage(value, text.toString());
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
		public WikipediaPage createValue() {
			return new WikipediaPage();
		}

		/**
		 * Returns the current position in the input.
		 */
		public long getPos() throws IOException {
			return reader.getPos();
		}

		/**
		 * Closes this InputSplit.
		 */
		public void close() throws IOException {
			reader.close();
		}

		/**
		 * Returns progress on how much input has been consumed.
		 */
		public float getProgress() throws IOException {
			return ((float) (reader.getPos() - reader.getStart()))
					/ ((float) (reader.getEnd() - reader.getStart()));
		}
	}
}
