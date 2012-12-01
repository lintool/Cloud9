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

package edu.umd.cloud9.io;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

import edu.umd.cloud9.io.pair.PairOfWritables;

/**
 * Class containing a number of utility methods for manipulating SequenceFiles.
 */
public class SequenceFileUtils {
  private static final TupleFactory TUPLE_FACTORY = TupleFactory.getInstance();

	private SequenceFileUtils() {}

	public static <K extends Writable, V extends Writable> List<PairOfWritables<K, V>> readFile(Path path)
	    throws IOException {
		FileSystem fs;
		fs = FileSystem.get(new Configuration());

		return readFile(path, fs, Integer.MAX_VALUE);
	}

	public static <K extends Writable, V extends Writable>
	    List<PairOfWritables<K, V>> readFile(Path path, int max) throws IOException {
		FileSystem fs;
		fs = FileSystem.get(new Configuration());

		return readFile(path, fs, max);
	}

	public static <K extends Writable, V extends Writable>
	    List<PairOfWritables<K, V>> readFile(Path path, FileSystem fs) throws IOException {
		return readFile(path, fs, Integer.MAX_VALUE);
	}

	/**
	 * Reads key-value pairs from a SequenceFile, up to a maximum number.
	 *
	 * @param path path to file
	 * @param max maximum of key-value pairs to read
	 * @return list of key-value pairs
	 */
	@SuppressWarnings("unchecked")
	public static <K extends Writable, V extends Writable>
	    List<PairOfWritables<K, V>> readFile(Path path, FileSystem fs, int max) throws IOException {
		List<PairOfWritables<K, V>> list = new ArrayList<PairOfWritables<K, V>>();

		try {
			int k = 0;
			SequenceFile.Reader reader = new SequenceFile.Reader(fs, path, fs.getConf());

			K key;
			V value;

      if (Tuple.class.isAssignableFrom(reader.getKeyClass())) {
        key = (K) TUPLE_FACTORY.newTuple();
      } else {
        key = (K) reader.getKeyClass().newInstance();
      }

      if (Tuple.class.isAssignableFrom(reader.getValueClass())) {
        value = (V) TUPLE_FACTORY.newTuple();
      } else {
        value = (V) reader.getValueClass().newInstance();
      }

			while (reader.next(key, value)) {
				k++;
				list.add(new PairOfWritables<K, V>(key, value));
				if (k >= max) {
					break;
				}

				// Create new objects, because the key, value gets reused
	      if (Tuple.class.isAssignableFrom(reader.getKeyClass())) {
	        key = (K) TUPLE_FACTORY.newTuple();
	      } else {
	        key = (K) reader.getKeyClass().newInstance();
	      }

	      if (Tuple.class.isAssignableFrom(reader.getValueClass())) {
	        value = (V) TUPLE_FACTORY.newTuple();
	      } else {
	        value = (V) reader.getValueClass().newInstance();
	      }
			}
			reader.close();
		} catch (IllegalAccessException e) {
			throw new RuntimeException("Error reading SequenceFile: " + e);
		} catch (InstantiationException e) {
      throw new RuntimeException("Error reading SequenceFile: " + e);
    }

		return list;
	}

	public static <K extends Writable, V extends Writable>
	    SortedMap<K, V> readFileIntoMap(Path path) throws IOException {
		FileSystem fs;
		fs = FileSystem.get(new Configuration());

		return readFileIntoMap(path, fs, Integer.MAX_VALUE);
	}

	public static <K extends Writable, V extends Writable> SortedMap<K, V>
	    readFileIntoMap(Path path, int max) throws IOException {
		FileSystem fs;
		fs = FileSystem.get(new Configuration());

		return readFileIntoMap(path, fs, max);
	}

	public static <K extends Writable, V extends Writable> SortedMap<K, V>
	    readFileIntoMap(Path path,	FileSystem fs) throws IOException {
		return readFileIntoMap(path, fs, Integer.MAX_VALUE);
	}

	public static <K extends Writable, V extends Writable> SortedMap<K, V>
	    readFileIntoMap(Path path, FileSystem fs, int max) throws IOException {
		SortedMap<K, V> map = new TreeMap<K, V>();

		for ( PairOfWritables<K,V> pair : SequenceFileUtils.<K, V>readFile(path, fs, max)) {
			map.put(pair.getLeftElement(), pair.getRightElement());
		}
		return map;
	}

	public static <K extends Writable, V extends Writable> List<PairOfWritables<K, V>> readDirectory(Path path) {
		FileSystem fs;
		try {
			fs = FileSystem.get(new Configuration());
		} catch (IOException e) {
			throw new RuntimeException("Unable to access the file system!");
		}

		return readDirectory(path, fs, Integer.MAX_VALUE);
	}

	/**
	 * Reads key-value pairs from a directory containing SequenceFiles. A
	 * maximum number of key-value pairs is read from each SequenceFile.
	 *
	 * @param path path to directory
	 * @param max maximum of key-value pairs to read per file
	 * @return list of key-value pairs
	 */
	public static <K extends Writable, V extends Writable> List<PairOfWritables<K, V>> readDirectory(Path path, FileSystem fs, int max) {
		List<PairOfWritables<K, V>> list = new ArrayList<PairOfWritables<K, V>>();

		try {
			FileStatus[] stat = fs.listStatus(path);
			for (int i = 0; i < stat.length; ++i) {

				// skip '_log' directory
				if (stat[i].getPath().getName().startsWith("_"))
					continue;

				List<PairOfWritables<K, V>> pairs = readFile(stat[i].getPath(), fs, max);
				list.addAll(pairs);
			}
		} catch (IOException e) {
			throw new RuntimeException("Error reading the file system!");
		}

		return list;
	}

	public static <K extends Writable> List<K> readKeys(Path path) {
		FileSystem fs;
		try {
			fs = FileSystem.get(new Configuration());
		} catch (IOException e) {
			throw new RuntimeException("Unable to access the file system!");
		}

		return readKeys(path, fs, Integer.MAX_VALUE);
	}

	public static <K extends Writable> List<K> readKeys(Path path, int max) {
		FileSystem fs;
		try {
			fs = FileSystem.get(new Configuration());
		} catch (IOException e) {
			throw new RuntimeException("Unable to access the file system!");
		}

		return readKeys(path, fs, max);
	}

	public static <K extends Writable> List<K> readKeys(Path path, FileSystem fs) {
		return readKeys(path, fs, Integer.MAX_VALUE);
	}

	@SuppressWarnings("unchecked")
	public static <K extends Writable> List<K> readKeys(Path path, FileSystem fs, int max) {
		List<K> list = new ArrayList<K>();

		try {
			int k = 0;
			SequenceFile.Reader reader = new SequenceFile.Reader(fs, path, fs.getConf());

			K key = (K) reader.getKeyClass().newInstance();
			Writable value = (Writable) reader.getValueClass().newInstance();
			while (reader.next(key, value)) {
				k++;
				list.add(key);
				if (k >= max) {
					break;
				}

				key = (K) reader.getKeyClass().newInstance();
			}
			reader.close();
		} catch (Exception e) {
			throw new RuntimeException("Error reading SequenceFile " + path);
		}

		return list;
	}

	public static <V extends Writable> List<V> readValues(Path path) {
		FileSystem fs;
		try {
			fs = FileSystem.get(new Configuration());
		} catch (IOException e) {
			throw new RuntimeException("Unable to access the file system!");
		}

		return readValues(path, fs, Integer.MAX_VALUE);
	}

	public static <V extends Writable> List<V> readValues(Path path, int max) {
		FileSystem fs;
		try {
			fs = FileSystem.get(new Configuration());
		} catch (IOException e) {
			throw new RuntimeException("Unable to access the file system!");
		}

		return readValues(path, fs, max);
	}

	public static <V extends Writable> List<V> readValues(Path path, FileSystem fs) {
		return readValues(path, fs, Integer.MAX_VALUE);
	}

	@SuppressWarnings("unchecked")
	public static <V extends Writable> List<V> readValues(Path path, FileSystem fs, int max) {
		List<V> list = new ArrayList<V>();

		try {
			int k = 0;
			SequenceFile.Reader reader = new SequenceFile.Reader(fs, path, fs.getConf());

			Writable key = (Writable) reader.getKeyClass().newInstance();
			V value = (V) reader.getValueClass().newInstance();

			while (reader.next(key, value)) {
				k++;
				list.add(value);
				if (k >= max) {
					break;
				}

				value = (V) reader.getValueClass().newInstance();
			}
			reader.close();
		} catch (Exception e) {
			throw new RuntimeException("Error reading SequenceFile " + path);
		}

		return list;
	}
}
