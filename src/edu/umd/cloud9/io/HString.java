package edu.umd.cloud9.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class HString implements WritableComparable {

	private String mString;

	public HString() {
	}
	
	public HString(String s) {
		mString = s;
	}

	public String getString() {
		return mString;
	}

	public void setString(String s) {
		mString = s;
	}

	public void readFields(DataInput in) throws IOException {
		int len = in.readInt();

		byte[] buf = new byte[len];
		in.readFully(buf);
		mString = new String(buf);
	}

	public void write(DataOutput out) throws IOException {
		byte[] buf = mString.getBytes();

		out.writeInt(buf.length);
		out.write(buf);
	}

	public int compareTo(Object obj) {
		return mString.compareTo(((HString) obj).getString());
	}

	public String toString() {
		return mString;
	}
}
