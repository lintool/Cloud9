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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.log4j.Logger;

/**
 * A simple {@link org.apache.hadoop.mapreduce.InputFormat} for XML documents ({@code
 * org.apache.hadoop.mapreduce} API). The class recognizes begin-of-document and end-of-document
 * tags only: everything between those delimiting tags is returned in an uninterpreted {@code Text}
 * object.
 *
 * @author Jimmy Lin
 */
public class XMLInputFormat extends TextInputFormat {
  public static final String START_TAG_KEY = "xmlinput.start";
  public static final String END_TAG_KEY = "xmlinput.end";

  /**
   * Create a record reader for a given split. The framework will call
   * {@link RecordReader#initialize(InputSplit, TaskAttemptContext)} before
   * the split is used.
   *
   * @param split the split to be read
   * @param context the information about the task
   * @return a new record reader
   * @throws IOException
   * @throws InterruptedException
   */
  @Override
  public RecordReader<LongWritable, Text> createRecordReader(InputSplit split,
      TaskAttemptContext context) {
    return new XMLRecordReader();
  }

  /**
   * Simple {@link org.apache.hadoop.mapreduce.RecordReader} for XML documents ({@code
   * org.apache.hadoop.mapreduce} API). Recognizes begin-of-document and end-of-document tags only:
   * everything between those delimiting tags is returned in a {@link Text} object.
   *
   * @author Jimmy Lin
   */
  public static class XMLRecordReader extends RecordReader<LongWritable, Text> {
    private static final Logger LOG = Logger.getLogger(XMLRecordReader.class);

    private byte[] startTag;
    private byte[] endTag;
    private long start;
    private long end;
    private long pos;
    private DataInputStream fsin = null;
    private DataOutputBuffer buffer = new DataOutputBuffer();

    private long recordStartPos;

    private final LongWritable key = new LongWritable();
    private final Text value = new Text();

    /**
     * Called once at initialization.
     *
     * @param input the split that defines the range of records to read
     * @param context the information about the task
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public void initialize(InputSplit input, TaskAttemptContext context)
        throws IOException, InterruptedException {
      Configuration conf = context.getConfiguration();
      if (conf.get(START_TAG_KEY) == null || conf.get(END_TAG_KEY) == null)
        throw new RuntimeException("Error! XML start and end tags unspecified!");

      startTag = conf.get(START_TAG_KEY).getBytes("utf-8");
      endTag = conf.get(END_TAG_KEY).getBytes("utf-8");

      FileSplit split = (FileSplit) input;
      start = split.getStart();
      Path file = split.getPath();

      CompressionCodecFactory compressionCodecs = new CompressionCodecFactory(conf);
      CompressionCodec codec = compressionCodecs.getCodec(file);

      FileSystem fs = file.getFileSystem(conf);

      if (codec != null) {
        LOG.info("Reading compressed file " + file + "...");
        fsin = new DataInputStream(codec.createInputStream(fs.open(file)));

        end = Long.MAX_VALUE;
      } else {
        LOG.info("Reading uncompressed file " + file + "...");
        FSDataInputStream fileIn = fs.open(file);

        fileIn.seek(start);
        fsin = fileIn;

        end = start + split.getLength();
      }

      recordStartPos = start;

      // Because input streams of gzipped files are not seekable, we need to keep track of bytes
      // consumed ourselves.
      pos = start;
    }

    /**
     * Read the next key, value pair.
     *
     * @return {@code true} if a key/value pair was read
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
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
            // Because input streams of gzipped files are not seekable, we need to keep track of
            // bytes consumed ourselves.

            // This is a sanity check to make sure our internal computation of bytes consumed is
            // accurate. This should be removed later for efficiency once we confirm that this code
            // works correctly.

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

    /**
     * Returns the current key.
     *
     * @return the current key or {@code null} if there is no current key
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public LongWritable getCurrentKey() throws IOException, InterruptedException {
      return key;
    }

    /**
     * Returns the current value.
     *
     * @return current value
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public Text getCurrentValue() throws IOException, InterruptedException {
      return value;
    }

    /**
     * Closes the record reader.
     */
    @Override
    public void close() throws IOException {
      fsin.close();
    }

    /**
     * The current progress of the record reader through its data.
     *
     * @return a number between 0.0 and 1.0 that is the fraction of the data read
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public float getProgress() throws IOException {
      return ((float) (pos - start)) / ((float) (end - start));
    }

    private boolean readUntilMatch(byte[] match, boolean withinBlock)
        throws IOException {
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
