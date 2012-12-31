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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import edu.umd.cloud9.collection.IndexableFileInputFormat;
import edu.umd.cloud9.collection.WebDocument;
import edu.umd.cloud9.collection.XMLInputFormatOld;
import edu.umd.cloud9.collection.XMLInputFormat.XMLRecordReader;


public class TrecDocumentInputFormat extends
    IndexableFileInputFormat<LongWritable, WebDocument> {

  @Override
  public RecordReader<LongWritable, WebDocument> createRecordReader(
      InputSplit split, TaskAttemptContext context) throws IOException,
      InterruptedException {
    return new TrecDocumentRecordReader();
  }

  public static class TrecDocumentRecordReader extends
      RecordReader<LongWritable, WebDocument> {
    private final XMLRecordReader reader = new XMLRecordReader();
    private final TrecDocument doc = new TrecDocument();

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context)
        throws IOException, InterruptedException {
      Configuration conf = context.getConfiguration();
      conf.set(XMLInputFormatOld.START_TAG_KEY, TrecDocument.XML_START_TAG);
      conf.set(XMLInputFormatOld.END_TAG_KEY, TrecDocument.XML_END_TAG);

      reader.initialize(split, context);
    }

    @Override
    public LongWritable getCurrentKey() throws IOException, InterruptedException {
      return reader.getCurrentKey();
    }

    @Override
    public WebDocument getCurrentValue() throws IOException, InterruptedException {
      TrecDocument.readDocument(doc, reader.getCurrentValue().toString());
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
