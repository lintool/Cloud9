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
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/**
 * Class containing a number of utility methods for manipulating SequenceFiles.
 */
public class SequenceFileUtils {

	private SequenceFileUtils() {
	}

	public static List<KeyValuePair<WritableComparable, Writable>> readFile(String path) {
		return readFile(new Path(path), Integer.MAX_VALUE);
	}

	public static List<KeyValuePair<WritableComparable, Writable>> readFile(Path path) {
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
	public static List<KeyValuePair<WritableComparable, Writable>> readFile(String path, int max) {
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
	public static List<KeyValuePair<WritableComparable, Writable>> readFile(Path path, int max) {

		List<KeyValuePair<WritableComparable, Writable>> list = new ArrayList<KeyValuePair<WritableComparable, Writable>>();

		try {
			int k = 0;

			Configuration config = new Configuration();
			FileSystem fs = FileSystem.get(config);
			SequenceFile.Reader reader = new SequenceFile.Reader(fs, path, config);

			WritableComparable key = (WritableComparable) reader.getKeyClass().newInstance();
			Writable value = (Writable) reader.getValueClass().newInstance();
			
			while (reader.next(key, value)) {
				k++;
				list.add(new KeyValuePair<WritableComparable, Writable>(key, value));
				if (k >= max)
					break;

				key = (WritableComparable) reader.getKeyClass().newInstance();
				value = (Writable) reader.getValueClass().newInstance();
			}
			reader.close();
		} catch (Exception e) {
			e.printStackTrace();
		}

		Collections.sort(list, new Comparator<KeyValuePair<WritableComparable, Writable>>() {
			@SuppressWarnings("unchecked")
			public int compare(KeyValuePair<WritableComparable, Writable> e1,
					KeyValuePair<WritableComparable, Writable> e2) {
				return e1.getKey().compareTo(e2.getKey());
			}
		});

		return list;
	}

	@SuppressWarnings("unchecked")
	public static List<Writable> readFile(String s, WritableComparable theKey, int max) {
		Path path = new Path(s);
		List<Writable> list = new ArrayList<Writable>();

		try {
			int k = 0;

			Configuration config = new Configuration();
			FileSystem fs = FileSystem.get(config);
			SequenceFile.Reader reader = new SequenceFile.Reader(fs, path, config);

			WritableComparable key = (WritableComparable) reader.getKeyClass().newInstance();
			Writable value = (Writable) reader.getValueClass().newInstance();

			while (reader.next(key, value)) {
				k++;
				if((theKey.getClass()==Text.class && ((Text)theKey).toString().equals("")) || key.compareTo(theKey)==0){
					list.add(value);	
				}
				if (max!=-1 && k >= max)
					break;

				key = (WritableComparable) reader.getKeyClass().newInstance();
				value = (Writable) reader.getValueClass().newInstance();
			}
			reader.close();
		} catch (Exception e) {
			e.printStackTrace();
		}

		return list;
	}
	
	public static List<KeyValuePair<WritableComparable, Writable>> readDirectory(String path) {
		return readDirectory(path, Integer.MAX_VALUE);
	}

	public static List<KeyValuePair<WritableComparable, Writable>> readDirectory(Path path) {
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
	public static List<KeyValuePair<WritableComparable, Writable>> readDirectory(String path,
			int max) {
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
	public static List<KeyValuePair<WritableComparable, Writable>> readDirectory(Path path, int max) {
		List<KeyValuePair<WritableComparable, Writable>> list = new ArrayList<KeyValuePair<WritableComparable, Writable>>();

		try {
			FileSystem fileSys = FileSystem.get(new Configuration());
			FileStatus[] stat = fileSys.listStatus(path);
			for (int i = 0; i < stat.length; ++i) {

				// skip '_log' directory
				if (stat[i].getPath().getName().startsWith("_"))
					continue;

				System.out.println("Reading " + stat[i].getPath().getName() + "...");
				List<KeyValuePair<WritableComparable, Writable>> pairs = readFile(
						stat[i].getPath(), max);

				list.addAll(pairs);

			}
		} catch (IOException e) {
			e.printStackTrace();
		}

		Collections.sort(list, new Comparator<KeyValuePair<WritableComparable, Writable>>() {
			@SuppressWarnings("unchecked")
			public int compare(KeyValuePair<WritableComparable, Writable> e1,
					KeyValuePair<WritableComparable, Writable> e2) {
				return e1.getKey().compareTo(e2.getKey());
			}
		});

		return list;
	}

	public static HashMap<Writable,Writable> readFileIntoMap(String s, int max) {
		Path path = new Path(s);
		HashMap<Writable,Writable> list = new HashMap<Writable,Writable>();

		try {
			int k = 0;

			Configuration config = new Configuration();
			FileSystem fs = FileSystem.get(config);
			SequenceFile.Reader reader = new SequenceFile.Reader(fs, path, config);

			Writable key = (Writable) reader.getKeyClass().newInstance();
			Writable value = (Writable) reader.getValueClass().newInstance();

			while (reader.next(key, value)) {
				k++;
				list.put(key, value);
				
				if (max!=-1 && k >= max)
					break;

				key = (Writable) reader.getKeyClass().newInstance();
				value = (Writable) reader.getValueClass().newInstance();
			}
			reader.close();
		} catch (Exception e) {
			e.printStackTrace();
		}

		return list;	
	}

	public static List<Writable> readLocalFile(Path path) {
		List<Writable> list = new ArrayList<Writable>();

		try {
			Configuration config = new Configuration();
			FileSystem fs = FileSystem.getLocal(config);
			SequenceFile.Reader reader = new SequenceFile.Reader(fs, path, config);

			WritableComparable key = (WritableComparable) reader.getKeyClass().newInstance();
			Writable value = (Writable) reader.getValueClass().newInstance();
			
			while (reader.next(key, value)) {
				list.add(value);
				
				key = (WritableComparable) reader.getKeyClass().newInstance();
				value = (Writable) reader.getValueClass().newInstance();
			}
			reader.close();
		} catch (Exception e) {
			e.printStackTrace();
		}

		return list;
	}

	public static List<KeyValuePair<WritableComparable, Writable>> readLocalFileInPairs(Path path){

		List<KeyValuePair<WritableComparable, Writable>> list = new ArrayList<KeyValuePair<WritableComparable, Writable>>();

		try {

			Configuration config = new Configuration();
			FileSystem fs = FileSystem.getLocal(config);
			SequenceFile.Reader reader = new SequenceFile.Reader(fs, path, config);

			WritableComparable key = (WritableComparable) reader.getKeyClass().newInstance();
			Writable value = (Writable) reader.getValueClass().newInstance();
			
			while (reader.next(key, value)) {
				list.add(new KeyValuePair<WritableComparable, Writable>(key, value));
		
				key = (WritableComparable) reader.getKeyClass().newInstance();
				value = (Writable) reader.getValueClass().newInstance();
			}
			reader.close();
		} catch (Exception e) {
			e.printStackTrace();
		}

		Collections.sort(list, new Comparator<KeyValuePair<WritableComparable, Writable>>() {
			@SuppressWarnings("unchecked")
			public int compare(KeyValuePair<WritableComparable, Writable> e1,
					KeyValuePair<WritableComparable, Writable> e2) {
				return e1.getKey().compareTo(e2.getKey());
			}
		});

		return list;
	}
}
