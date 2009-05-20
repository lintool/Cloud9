package edu.umd.cloud9.mapred;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.mapred.InputSplit;

public class NullInputSplit implements InputSplit {

	public long getLength() {
		return 0;
	}

	public String[] getLocations() {
		String[] locs = {};
		return locs;
	}

	public void readFields(DataInput in) throws IOException {
	}

	public void write(DataOutput out) throws IOException {
	}
}
