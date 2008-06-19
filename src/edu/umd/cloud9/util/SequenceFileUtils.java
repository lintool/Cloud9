package edu.umd.cloud9.util;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.JobConf;

import edu.umd.cloud9.util.KeyValuePair;

public class SequenceFileUtils {

	public static void readDirectory(String inPath, int max) {
		try {
			JobConf config = new JobConf();
			Writable key, value;
			FileSystem fileSys = FileSystem.get(config);
			Path p = new Path(inPath);
			Path[] files = fileSys.listPaths(p);
			int k = 0;

			for (int i = 0; i < files.length; i++) {
				SequenceFile.Reader reader = new SequenceFile.Reader(fileSys, files[i], config);
				System.out.println("reading " + files[i]);

				key = (Writable) reader.getKeyClass().newInstance();
				value = (Writable) reader.getValueClass().newInstance();

				while (reader.next(key, value)) {
					System.out.println(key + " -> " + value);
					k++;

					if (k > max)
						break;
				}
				reader.close();
				System.out.println("records read: " + k);
			}

			System.out.println("total records read: " + k);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static List<KeyValuePair<? extends WritableComparable, ? extends Writable>> readFile(
			String path, int max) {

		List<KeyValuePair<? extends WritableComparable, ? extends Writable>> list = new ArrayList<KeyValuePair<? extends WritableComparable, ? extends Writable>>();

		try {
			JobConf config = new JobConf();
			WritableComparable key;
			Writable value;
			FileSystem fileSys = FileSystem.get(config);
			int k = 0;

			SequenceFile.Reader reader = new SequenceFile.Reader(fileSys, new Path(path), config);

			key = (WritableComparable) reader.getKeyClass().newInstance();
			value = (Writable) reader.getValueClass().newInstance();

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
}
