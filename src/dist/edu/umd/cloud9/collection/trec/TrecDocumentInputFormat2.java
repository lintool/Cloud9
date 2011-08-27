package edu.umd.cloud9.collection.trec;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import edu.umd.cloud9.collection.IndexableFileInputFormat2;
import edu.umd.cloud9.collection.XMLInputFormat;
import edu.umd.cloud9.collection.XMLInputFormat2.XMLRecordReader;

public class TrecDocumentInputFormat2 extends
    IndexableFileInputFormat2<LongWritable, TrecDocument> {

  @Override
  public RecordReader<LongWritable, TrecDocument> createRecordReader(
      InputSplit split, TaskAttemptContext context) throws IOException,
      InterruptedException {
    return new TrecDocumentRecordReader();
  }

  public static class TrecDocumentRecordReader extends
      RecordReader<LongWritable, TrecDocument> {
    private final XMLRecordReader reader = new XMLRecordReader();
    private final TrecDocument doc = new TrecDocument();

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context)
        throws IOException, InterruptedException {
      Configuration conf = context.getConfiguration();
      conf.set(XMLInputFormat.START_TAG_KEY, TrecDocument.XML_START_TAG);
      conf.set(XMLInputFormat.END_TAG_KEY, TrecDocument.XML_END_TAG);

      reader.initialize(split, context);
    }

    @Override
    public LongWritable getCurrentKey() throws IOException, InterruptedException {
      return reader.getCurrentKey();
    }

    @Override
    public TrecDocument getCurrentValue() throws IOException, InterruptedException {
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
