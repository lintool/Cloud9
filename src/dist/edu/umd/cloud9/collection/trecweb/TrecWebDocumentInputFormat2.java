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

package edu.umd.cloud9.collection.trecweb;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import edu.umd.cloud9.collection.IndexableFileInputFormat2;
import edu.umd.cloud9.collection.XMLInputFormat2;
import edu.umd.cloud9.collection.XMLInputFormat2.XMLRecordReader;
import edu.umd.cloud9.collection.generic.WebDocument;

/**
 * Hadoop <code>InputFormat</code> for processing the TREC collection.
 * 
 * @author Jimmy Lin
 */
public class TrecWebDocumentInputFormat2 extends IndexableFileInputFormat2<LongWritable, WebDocument> {

  /**
   * Don't allow the files to be split!
   */
  @Override
  protected boolean isSplitable(JobContext context, Path filename) {
    // ensure the input files are not splittable!
    return false;
  }

  /**
   * Returns a <code>RecordReader</code> for this <code>InputFormat</code>.
   */
  @Override
  public RecordReader<LongWritable, WebDocument> createRecordReader(
      InputSplit split, TaskAttemptContext context) throws IOException,
      InterruptedException {
    return new TrecWebRecordReader();
  }

  /**
   * Hadoop <code>RecordReader</code> for reading TREC-formatted documents.
   */
  public static class TrecWebRecordReader extends RecordReader<LongWritable, WebDocument> {

    private XMLRecordReader reader = new XMLRecordReader();
    private final TrecWebDocument doc = new TrecWebDocument();

    /**
     * Creates a <code>TrecDocumentRecordReader</code>.
     */
    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
      Configuration conf = context.getConfiguration();

      conf.set(XMLInputFormat2.START_TAG_KEY, TrecWebDocument.XML_START_TAG);
      conf.set(XMLInputFormat2.END_TAG_KEY, TrecWebDocument.XML_END_TAG);

      reader.initialize(split, context);
    }

    @Override
    public LongWritable getCurrentKey() throws IOException, InterruptedException {
      return reader.getCurrentKey();
    }

    @Override
    public WebDocument getCurrentValue() throws IOException, InterruptedException {
      TrecWebDocument.readDocument(doc, reader.getCurrentValue().toString());
      return doc;
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
      return reader.nextKeyValue();
    }

    @Override
    public void close() throws IOException {
      reader.close();
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
      return reader.getProgress();
    }

  }
}
