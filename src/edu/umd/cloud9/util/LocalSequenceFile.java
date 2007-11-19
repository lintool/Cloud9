package edu.umd.cloud9.util;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapred.JobConf;

public class LocalSequenceFile {

	private LocalSequenceFile() {
	}

	public static SequenceFile.Reader createReader(String file) {
		JobConf config = new JobConf();
		SequenceFile.Reader reader = null;

		try {
			reader = new SequenceFile.Reader(FileSystem.get(config), new Path(
					file), config);
		} catch (Exception e) {
			e.printStackTrace();
		}

		return reader;
	}

}
