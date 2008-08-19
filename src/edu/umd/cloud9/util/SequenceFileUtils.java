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
import org.apache.hadoop.mapred.JobConf;

public class SequenceFileUtils {

	public static List<KeyValuePair<WritableComparable, Writable>> readFile(String path, int max) {

		return readFile(new Path(path), max);
	}

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

		return list;
	}

	public static List<KeyValuePair<WritableComparable, Writable>> readDirectory(String path,
			int max) {

		return readDirectory(new Path(path), max);
	}

	public static List<KeyValuePair<WritableComparable, Writable>> readDirectory(Path path, int max) {

		List<KeyValuePair<WritableComparable, Writable>> list = new ArrayList<KeyValuePair<WritableComparable, Writable>>();

		JobConf config = new JobConf();
		try {
			FileSystem fileSys = FileSystem.get(config);
			FileStatus[] stat = fileSys.listStatus(path);
			for (int i = 0; i < stat.length; ++i) {

				// skip '_log' directory
				if (stat[i].getPath().getName().startsWith("_"))
					continue;

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
}
