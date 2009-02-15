package edu.umd.cloud9.data;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public interface DocnoMapping {
	public int getDocno(String docid);

	public void loadMapping(Path f, FileSystem fs) throws IOException;
}
