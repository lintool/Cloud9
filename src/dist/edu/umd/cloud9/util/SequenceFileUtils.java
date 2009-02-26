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

package edu.umd.cloud9.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.log4j.Logger;

/**
 * Class containing a number of utility methods for manipulating SequenceFiles.
 */
public class SequenceFileUtils {

	private static final Logger sLogger = Logger.getLogger(SequenceFileUtils.class);

	private SequenceFileUtils() {
	}

	public static <K extends WritableComparable, V extends Writable> List<KeyValuePair<K, V>> readFile(
			String path) {
		return readFile(new Path(path), Integer.MAX_VALUE);
	}

	public static <K extends WritableComparable, V extends Writable> List<KeyValuePair<K, V>> readFile(
			Path path) {
		return readFile(path, Integer.MAX_VALUE);
	}

	/**
	 * Reads key-value pairs from a SequenceFile, up to a maximum number.
	 * 
	 * @param path
	 *            path to file (as a String)
	 * @param max
	 *            maximum of key-value pairs to read
	 * @return list of key-value pairs
	 */
	public static <K extends WritableComparable, V extends Writable> List<KeyValuePair<K, V>> readFile(
			String path, int max) {
		return readFile(new Path(path), max);
	}

	/**
	 * Reads key-value pairs from a SequenceFile, up to a maximum number.
	 * 
	 * @param path
	 *            path to file (as a Path)
	 * @param max
	 *            maximum of key-value pairs to read
	 * @return list of key-value pairs
	 */
	@SuppressWarnings("unchecked")
	public static <K extends WritableComparable, V extends Writable> List<KeyValuePair<K, V>> readFile(
			Path path, int max) {

		List<KeyValuePair<K, V>> list = new ArrayList<KeyValuePair<K, V>>();

		try {
			int k = 0;

			Configuration config = new Configuration();
			FileSystem fs = FileSystem.get(config);
			SequenceFile.Reader reader = new SequenceFile.Reader(fs, path, config);

			K key = (K) reader.getKeyClass().newInstance();
			V value = (V) reader.getValueClass().newInstance();

			while (reader.next(key, value)) {
				k++;

				list.add(new KeyValuePair(key, value));
				if (k >= max)
					break;

				key = (K) reader.getKeyClass().newInstance();
				value = (V) reader.getValueClass().newInstance();
			}
			reader.close();
		} catch (Exception e) {
			e.printStackTrace();
		}

		Collections.sort(list, new Comparator<KeyValuePair<K, V>>() {
			@SuppressWarnings("unchecked")
			public int compare(KeyValuePair<K, V> e1, KeyValuePair<K, V> e2) {
				return e1.getKey().compareTo(e2.getKey());
			}
		});

		return list;
	}

	public static <K extends WritableComparable, V extends Writable> List<KeyValuePair<K, V>> readDirectory(
			String path) {
		return readDirectory(path, Integer.MAX_VALUE);
	}

	public static <K extends WritableComparable, V extends Writable> List<KeyValuePair<K, V>> readDirectory(
			Path path) {
		return readDirectory(path, Integer.MAX_VALUE);
	}

	/**
	 * Reads key-value pairs from a directory containing SequenceFiles. A
	 * maximum number of key-value pairs is read from each SequenceFile.
	 * 
	 * @param path
	 *            path to directory (as a String)
	 * @param max
	 *            maximum of key-value pairs to read per file
	 * @return list of key-value pairs
	 */
	public static <K extends WritableComparable, V extends Writable> List<KeyValuePair<K, V>> readDirectory(
			String path, int max) {
		return readDirectory(new Path(path), max);
	}

	/**
	 * Reads key-value pairs from a directory containing SequenceFiles. A
	 * maximum number of key-value pairs is read from each SequenceFile.
	 * 
	 * @param path
	 *            path to directory (as a Path)
	 * @param max
	 *            maximum of key-value pairs to read per file
	 * @return list of key-value pairs
	 */
	public static <K extends WritableComparable, V extends Writable> List<KeyValuePair<K, V>> readDirectory(
			Path path, int max) {
		List<KeyValuePair<K, V>> list = new ArrayList<KeyValuePair<K, V>>();

		try {
			FileSystem fileSys = FileSystem.get(new Configuration());
			FileStatus[] stat = fileSys.listStatus(path);
			for (int i = 0; i < stat.length; ++i) {

				// skip '_log' directory
				if (stat[i].getPath().getName().startsWith("_"))
					continue;

				sLogger.info("Reading " + stat[i].getPath().getName() + "...");
				List<KeyValuePair<K, V>> pairs = readFile(stat[i].getPath(), max);

				list.addAll(pairs);

			}
		} catch (IOException e) {
			e.printStackTrace();
		}

		Collections.sort(list, new Comparator<KeyValuePair<K, V>>() {
			@SuppressWarnings("unchecked")
			public int compare(KeyValuePair<K, V> e1, KeyValuePair<K, V> e2) {
				return e1.getKey().compareTo(e2.getKey());
			}
		});

		return list;
	}
}
