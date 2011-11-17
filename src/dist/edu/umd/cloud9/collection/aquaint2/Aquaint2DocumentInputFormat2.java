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

package edu.umd.cloud9.collection.aquaint2;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import edu.umd.cloud9.collection.IndexableFileInputFormat2;
import edu.umd.cloud9.collection.XMLInputFormat;
import edu.umd.cloud9.collection.XMLInputFormat2.XMLRecordReader;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class Aquaint2DocumentInputFormat2 extends
    IndexableFileInputFormat2<LongWritable, Aquaint2Document> {

  @Override
  public RecordReader<LongWritable, Aquaint2Document> createRecordReader(
      InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
    return new Aquaint2DocumentRecordReader();
  }

  public static class Aquaint2DocumentRecordReader extends
      RecordReader<LongWritable, Aquaint2Document> {
    private final XMLRecordReader reader = new XMLRecordReader();
    private final Aquaint2Document doc = new Aquaint2Document();

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context)
        throws IOException, InterruptedException {
      reader.initialize(split, context);
    }

    @Override
    public LongWritable getCurrentKey() throws IOException, InterruptedException {
      return reader.getCurrentKey();
    }

    @Override
    public Aquaint2Document getCurrentValue() throws IOException, InterruptedException {
      Aquaint2Document.readDocument(doc, reader.getCurrentValue().toString());
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
