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

package edu.umd.cloud9.collection;

import java.io.DataInputStream;
import java.io.IOException;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.log4j.Logger;

// solution for reading XML files, posted to the Hadoop users mailing list
// Re: map/reduce function on xml string - Colin Evans-2 Mar 04, 2008; 02:27pm
public class XMLInputFormat extends TextInputFormat {
	public static final String START_TAG_KEY = "xmlinput.start";
	public static final String END_TAG_KEY = "xmlinput.end";

	public void configure(JobConf jobConf) {
		super.configure(jobConf);
	}

	public RecordReader<LongWritable, Text> getRecordReader(InputSplit inputSplit, JobConf jobConf,
			Reporter reporter) throws IOException {
		return new XMLRecordReader((FileSplit) inputSplit, jobConf);
	}

	public static class XMLRecordReader implements RecordReader<LongWritable, Text> {
		private static final Logger sLogger = Logger.getLogger(XMLRecordReader.class);
				
		private byte[] startTag;
		private byte[] endTag;
		private long start;
		private long end;
		private long pos;
		private DataInputStream fsin = null;
		private DataOutputBuffer buffer = new DataOutputBuffer();

		private long recordStartPos;

		public XMLRecordReader(FileSplit split, JobConf jobConf) throws IOException {
			if (jobConf.get(START_TAG_KEY) == null || jobConf.get(END_TAG_KEY) == null)
				throw new RuntimeException("Error! XML start and end tags unspecified!");

			startTag = jobConf.get(START_TAG_KEY).getBytes("utf-8");
			endTag = jobConf.get(END_TAG_KEY).getBytes("utf-8");

			start = split.getStart();
			Path file = split.getPath();

			CompressionCodecFactory compressionCodecs = new CompressionCodecFactory(jobConf);
			CompressionCodec codec = compressionCodecs.getCodec(file);
			
			FileSystem fs = file.getFileSystem(jobConf);

			if (codec != null) {
				sLogger.info("Reading compressed file...");

				//InputStream tempStream = codec.createInputStream(fileIn);
				//fsin = new DataInputStream(tempStream);

				fsin = new DataInputStream(codec.createInputStream(fs.open(file)));
				
				end = Long.MAX_VALUE;
			} else {
				sLogger.info("Reading uncompressed file...");

				FSDataInputStream fileIn = fs.open(file);

				fileIn.seek(start);
				fsin = fileIn;
				
				end = start + split.getLength();
			}

			recordStartPos = start;

			// Because input streams of gzipped files are not seekable
			// (specifically, do not support getPos), we need to keep
			// track of bytes consumed ourselves.
			pos = start;
		}

		public boolean next(LongWritable key, Text value) throws IOException {			
			if (pos < end) {
				if (readUntilMatch(startTag, false)) {
					recordStartPos = pos - startTag.length;

					try {
						buffer.write(startTag);
						if (readUntilMatch(endTag, true)) {
							key.set(recordStartPos);
							value.set(buffer.getData(), 0, buffer.getLength());
							return true;
						}
					} finally {
						// Because input streams of gzipped files are not
						// seekable (specifically, do not support getPos), we
						// need to keep track of bytes consumed ourselves.

						// This is a sanity check to make sure our internal
						// computation of bytes consumed is accurate. This
						// should be removed later for efficiency once we
						// confirm that this code works correctly.

						if (fsin instanceof Seekable) {
							if (pos != ((Seekable) fsin).getPos()) {
								throw new RuntimeException("bytes consumed error!");
							}
						}

						buffer.reset();
					}
				}
			}
			return false;
		}

		public LongWritable createKey() {
			return new LongWritable();
		}

		public Text createValue() {
			return new Text();
		}

		public long getStart() {
			return start;
		}

		public long getEnd() {
			return end;
		}

		public long getPos() throws IOException {
			return pos;
		}

		public void close() throws IOException {
			fsin.close();
		}

		public float getProgress() throws IOException {
			return ((float) (pos - start)) / ((float) (end - start));
		}

		private boolean readUntilMatch(byte[] match, boolean withinBlock) throws IOException {
			int i = 0;
			while (true) {
				int b = fsin.read();
				// increment position (bytes consumed)
				pos++;

				// end of file:
				if (b == -1)
					return false;
				// save to buffer:
				if (withinBlock)
					buffer.write(b);

				// check if we're matching:
				if (b == match[i]) {
					i++;
					if (i >= match.length)
						return true;
				} else
					i = 0;
				// see if we've passed the stop point:
				if (!withinBlock && i == 0 && pos >= end)
					return false;
			}
		}
	}
}
