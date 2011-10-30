package edu.umd.cloud9.collection.generic;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import edu.umd.cloud9.collection.clue.ClueWarcInputFormat2;
import edu.umd.cloud9.collection.trecweb.TrecWebDocumentInputFormat2;

public class WebDocumentInputFormat extends FileInputFormat<LongWritable, WebDocument> {
  //if one collection's name contain name of
  // another in beginning, it should appear first.
  public static final String[] supported = { "clue", "trecweb" };
  public InputFormat<LongWritable, WebDocument> fileFormatAgent = null;

  public static boolean isSupport(String tgtCollection)
  {
    return (getSupportIndex(tgtCollection) != -1);
  }

  private static int getSupportIndex(String tgtCollection)
  {
    tgtCollection = tgtCollection.toLowerCase();
    for (int i = 0; i < supported.length; i++)
      if (tgtCollection.startsWith(supported[i]))
        return i;
    return -1;
  }

  @Override
  public RecordReader<LongWritable, WebDocument> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
    try {
      buildAgent(context.getConfiguration());
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
    return fileFormatAgent.createRecordReader(split, context);
  }

  @SuppressWarnings("unchecked")
  private void buildAgent(Configuration conf) throws ClassNotFoundException {
    String inputString = conf.get("inputString", "");
    boolean userSpecifiedClass = conf.getBoolean("userSpecifiedClass", false);	

    if(userSpecifiedClass)
    {
      Object obj = Class.forName(inputString).getInterfaces();
      if(!InputFormat.class.isInstance(obj)) {
        throw new RuntimeException(String.format("Specified class '%s' has to be sub-class of InputFormat<LongWritable, WebDocument>",inputString));
      }
      fileFormatAgent = (InputFormat<LongWritable, WebDocument>)obj;
      return;
    }

    int index = getSupportIndex(inputString);
    switch(index)
    {
    case 0:
      fileFormatAgent = new ClueWarcInputFormat2();
      break;
    case 1:
      fileFormatAgent = new TrecWebDocumentInputFormat2();
      break;
    default:
      throw new RuntimeException(String.format("Target collection '%s'not supported.",inputString));
    }
  }
}
