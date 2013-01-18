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
import edu.umd.cloud9.collection.IndexableFileInputFormatOld;
import edu.umd.cloud9.collection.XMLInputFormatOld;
import edu.umd.cloud9.collection.XMLInputFormatOld.XMLRecordReader;
import edu.umd.cloud9.collection.wikipedia.language.WikipediaPageFactory;

/**
 * Hadoop {@code InputFormat} for processing Wikipedia pages from the XML dumps.
 *
 * @author Jimmy Lin
 * @author Peter Exner
 */
public class WikipediaPageInputFormat extends IndexableFileInputFormatOld<LongWritable, WikipediaPage> {
	/**
	 * Returns a {@code RecordReader} for this {@code InputFormat}.
	 */
	public RecordReader<LongWritable, WikipediaPage> getRecordReader(InputSplit inputSplit,
			JobConf conf, Reporter reporter) throws IOException {
		return new WikipediaPageRecordReader((FileSplit) inputSplit, conf);
	}

	/**
	 * Hadoop {@code RecordReader} for reading Wikipedia pages from the XML dumps.
	 */
	public static class WikipediaPageRecordReader implements RecordReader<LongWritable, WikipediaPage> {
		private XMLRecordReader reader;
		private Text text = new Text();
		private LongWritable offset = new LongWritable();
		private String language;
		
		/**
		 * Creates a {@code WikipediaPageRecordReader}.
		 */
		public WikipediaPageRecordReader(FileSplit split, JobConf conf) throws IOException {
			conf.set(XMLInputFormatOld.START_TAG_KEY, WikipediaPage.XML_START_TAG);
			conf.set(XMLInputFormatOld.END_TAG_KEY, WikipediaPage.XML_END_TAG);
			
			language = conf.get("wiki.language");
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
			return WikipediaPageFactory.createWikipediaPage(language);
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
