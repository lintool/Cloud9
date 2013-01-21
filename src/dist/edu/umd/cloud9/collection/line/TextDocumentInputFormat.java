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

package edu.umd.cloud9.collection.line;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobConfigurable;
import org.apache.hadoop.mapred.LineRecordReader;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

/**
 * Hadoop <code>InputFormat</code> for processing a simple collection. Each
 * document of the collection consists of a single line of text: the docid,
 * followed by a tab, followed by the document contents. Note that the document
 * content cannot contain embedded tabs or newlines.
 * 
 * @author Jimmy Lin
 */
public class TextDocumentInputFormat extends FileInputFormat<LongWritable, TextDocument>
		implements JobConfigurable {

	private CompressionCodecFactory compressionCodecs = null;

	public void configure(JobConf conf) {
		compressionCodecs = new CompressionCodecFactory(conf);
	}

	protected boolean isSplitable(FileSystem fs, Path file) {
		return compressionCodecs.getCodec(file) == null;
	}

	public RecordReader<LongWritable, TextDocument> getRecordReader(
												InputSplit genericSplit, JobConf job,
												Reporter reporter)
			throws IOException {

		reporter.setStatus(genericSplit.toString());
		return new TextDocumentLineRecordReader(job, (FileSplit) genericSplit);
	}

	public static class TextDocumentLineRecordReader implements
			RecordReader<LongWritable, TextDocument> {

		private LineRecordReader mRecordReader;
		private Text mText;

		public TextDocumentLineRecordReader(Configuration job,
					FileSplit split) throws IOException {
			mRecordReader = new LineRecordReader(job, split);
			mText = new Text();
		}

		public LongWritable createKey() {
			return new LongWritable();
		}

		public TextDocument createValue() {
			return new TextDocument();
		}

		public synchronized long getPos() throws IOException {
			return mRecordReader.getPos();
		}

		public synchronized void close() throws IOException {
			mRecordReader.getPos();
		}

		public float getProgress() {
		    try{
			return mRecordReader.getProgress();
		    } catch (IOException e) {
			return 0.0f;
		    }
		}

		public synchronized boolean next(LongWritable key, TextDocument value) {
			boolean b;
			try {
				b = mRecordReader.next(key, mText);
			} catch (IOException e) {
				e.printStackTrace();
				return false;
			}

			if (b == true) {
				TextDocument.readDocument(value, mText.toString());
			}

			return b;
		}

	}
}
