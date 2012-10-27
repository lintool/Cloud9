package edu.umd.cloud9.mapred;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.SequenceFileInputFormat;

public class NoSplitSequenceFileInputFormat<K extends WritableComparable<?>, V extends Writable>
		extends SequenceFileInputFormat<K, V> {

	protected boolean isSplitable(FileSystem fs, Path filename) {
		return false;
	}
}
