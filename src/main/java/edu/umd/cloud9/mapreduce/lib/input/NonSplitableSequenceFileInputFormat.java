package edu.umd.cloud9.mapreduce.lib.input;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;

public class NonSplitableSequenceFileInputFormat<K, V> extends SequenceFileInputFormat<K, V> {
  protected boolean isSplitable(JobContext context, Path filename) {
    return false;
  }
}
