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

public class Aquaint2DocumentInputFormatOld extends
    IndexableFileInputFormatOld<LongWritable, Aquaint2Document> {

  public void configure(JobConf conf) {}

  public RecordReader<LongWritable, Aquaint2Document> getRecordReader(InputSplit inputSplit,
      JobConf conf, Reporter reporter) throws IOException {
    return new Aquaint2DocumentRecordReader((FileSplit) inputSplit, conf);
  }

  public static class Aquaint2DocumentRecordReader implements
      RecordReader<LongWritable, Aquaint2Document> {
    private final XMLRecordReader reader;
    private final Text text = new Text();
    private final LongWritable offset = new LongWritable();

    public Aquaint2DocumentRecordReader(FileSplit split, JobConf conf) throws IOException {
      conf.set(XMLInputFormatOld.START_TAG_KEY, Aquaint2Document.XML_START_TAG);
      conf.set(XMLInputFormatOld.END_TAG_KEY, Aquaint2Document.XML_END_TAG);

      reader = new XMLRecordReader(split, conf);
    }

    @Override
    public boolean next(LongWritable key, Aquaint2Document value) throws IOException {
      if (reader.next(offset, text) == false)
        return false;
      key.set(offset.get());
      Aquaint2Document.readDocument(value, text.toString());
      return true;
    }

    @Override
    public LongWritable createKey() {
      return new LongWritable();
    }

    @Override
    public Aquaint2Document createValue() {
      return new Aquaint2Document();
    }

    @Override
    public long getPos() throws IOException {
      return reader.getPos();
    }

    @Override
    public void close() throws IOException {
      reader.close();
    }

    @Override
    public float getProgress() throws IOException {
      return ((float) (reader.getPos() - reader.getStart()))
          / ((float) (reader.getEnd() - reader.getStart()));
    }
  }
}
