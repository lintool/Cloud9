package edu.umd.cloud9.collection.trecweb;

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


public class TrecWebDocumentInputFormat extends
    IndexableFileInputFormat<LongWritable, WebDocument> {

  @Override
  public RecordReader<LongWritable, WebDocument> createRecordReader(InputSplit split,
      TaskAttemptContext context) throws IOException, InterruptedException {
    return new TrecWebDocumentRecordReader();
  }

  public static class TrecWebDocumentRecordReader extends RecordReader<LongWritable, WebDocument> {
    private final XMLRecordReader reader = new XMLRecordReader();
    private final TrecWebDocument doc = new TrecWebDocument();

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException,
        InterruptedException {
      Configuration conf = context.getConfiguration();
      conf.set(XMLInputFormatOld.START_TAG_KEY, TrecWebDocument.XML_START_TAG);
      conf.set(XMLInputFormatOld.END_TAG_KEY, TrecWebDocument.XML_END_TAG);

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
