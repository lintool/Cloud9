/*
 * Cloud9: A Hadoop toolkit for working with big data
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
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

import edu.umd.cloud9.collection.IndexableFileInputFormatOld;
import edu.umd.cloud9.collection.XMLInputFormatOld;
import edu.umd.cloud9.collection.XMLInputFormatOld.XMLRecordReader;

/**
 * Hadoop {@link InputFormat} for processing the TREC collection.
 *
 * @author Jimmy Lin
 */
public class TrecDocumentInputFormatOld extends IndexableFileInputFormatOld<LongWritable, TrecDocument> {

  /**
   * Returns a {@code RecordReader} for this {@code InputFormat}.
   */
  public RecordReader<LongWritable, TrecDocument> getRecordReader(InputSplit inputSplit,
      JobConf conf, Reporter reporter) throws IOException {
    return new TrecDocumentRecordReader((FileSplit) inputSplit, conf);
  }

  /**
   * Hadoop {@link RecordReader} for reading TREC-formatted documents.
   */
  public static class TrecDocumentRecordReader
      implements RecordReader<LongWritable, TrecDocument> {
    private XMLRecordReader reader;
    private Text text = new Text();
    private LongWritable pos = new LongWritable();

    /**
     * Creates a {@code TrecDocumentRecordReader}.
     */
    public TrecDocumentRecordReader(FileSplit split, JobConf conf) throws IOException {
      conf.set(XMLInputFormatOld.START_TAG_KEY, TrecDocument.XML_START_TAG);
      conf.set(XMLInputFormatOld.END_TAG_KEY, TrecDocument.XML_END_TAG);

      reader = new XMLRecordReader(split, conf);
    }

    /**
     * Reads the next key-value pair.
     */
    public boolean next(LongWritable key, TrecDocument value) throws IOException {
      if (reader.next(pos, text) == false) {
        return false;
      }
      key.set(pos.get());
      TrecDocument.readDocument(value, text.toString());
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
      return reader.getPos();
    }

    /**
     * Closes this {@code InputSplit}.
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
