package edu.umd.cloud9.util;

import java.io.IOException;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class FSProperty {

	public static void writeInt(FileSystem fs, String path, int val) {
		try {
			FSDataOutputStream out = fs.create(new Path(path), true);
			out.writeInt(val);
			out.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static void writeFloat(FileSystem fs, String path, float val) {
		try {
			FSDataOutputStream out = fs.create(new Path(path), true);
			out.writeFloat(val);
			out.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static void writeString(FileSystem fs, String path, String val) {
		try {
			FSDataOutputStream out = fs.create(new Path(path), true);
			out.writeUTF(val);
			out.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static int readInt(FileSystem fs, String path) {
		try {
			FSDataInputStream in = fs.open(new Path(path));
			int val = in.readInt();
			in.close();
			return val;
		} catch (Exception e) {
			e.printStackTrace();
		}

		return 0;
	}

	public static float readFloat(FileSystem fs, String path) {
		try {
			FSDataInputStream in = fs.open(new Path(path));
			float val = in.readFloat();
			in.close();
			return val;
		} catch (Exception e) {
			e.printStackTrace();
		}

		return 0f;
	}

	public static String readString(FileSystem fs, String path) {
		try {
			FSDataInputStream in = fs.open(new Path(path));
			String val = in.readUTF();
			in.close();
			return val;
		} catch (Exception e) {
			e.printStackTrace();
		}

		return null;
	}

}
