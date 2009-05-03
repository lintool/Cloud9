package edu.umd.cloud9.collection;

import org.apache.hadoop.mapred.FileInputFormat;

/**
 * Abstract class representing a <code>FileInputFormat</code> for
 * <code>Indexable</code> objects.
 */
public abstract class IndexableFileInputFormat<K, V extends Indexable> extends
		FileInputFormat<K, V> {

}
