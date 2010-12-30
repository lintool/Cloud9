package edu.umd.cloud9.io;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
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

	public static void writeLong(FileSystem fs, String path, long val) {
		try {
			FSDataOutputStream out = fs.create(new Path(path), true);
			out.writeLong(val);
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
			throw new RuntimeException("Unable to read property at " + path);
		}
	}

	public static long readLong(FileSystem fs, String path) {
		try {
			FSDataInputStream in = fs.open(new Path(path));
			long val = in.readLong();
			in.close();
			return val;
		} catch (Exception e) {
			throw new RuntimeException("Unable to read property at " + path);
		}
	}

	public static float readFloat(FileSystem fs, String path) {
		try {
			FSDataInputStream in = fs.open(new Path(path));
			float val = in.readFloat();
			in.close();
			return val;
		} catch (Exception e) {
			throw new RuntimeException("Unable to read property at " + path);
		}
	}

	public static String readString(FileSystem fs, String path) {
		try {
			FSDataInputStream in = fs.open(new Path(path));
			String val = in.readUTF();
			in.close();
			return val;
		} catch (Exception e) {
			throw new RuntimeException("Unable to read property at " + path);
		}
	}

	public static void main(String[] args) throws IOException {
		if (args.length < 3) {
			System.out.println("args: (read|write) (int|long|float|String) [file] (value)");
			System.exit(-1);
		}

		FileSystem fs = FileSystem.get(new Configuration());
		if (args[0].equals("read")) {
			if (args[1].equals("int")) {
				System.out.println("reading int from " + args[2]);
				System.out.println(readInt(fs, args[2]));
			} else if (args[1].equals("float")) {
				System.out.println("reading float from " + args[2]);
				System.out.println(readFloat(fs, args[2]));
			} else if (args[1].equals("long")) {
				System.out.println("reading long from " + args[2]);
				System.out.println(readLong(fs, args[2]));
			} else if (args[1].equals("String")) {
				System.out.println("reading String from " + args[2]);
				System.out.println(readString(fs, args[2]));
			} else {
				System.out.println("unknown read type");
				System.out.println("args: read (int|long|float|String) [file]");
				System.exit(-1);
			}
		} else if (args[0].equals("write")) {
			if (args[1].equals("int")) {
				int i = Integer.parseInt(args[3]);
				System.out.println("writing int \"" + i + "\" to " + args[2]);
				writeInt(fs, args[2], i);
			} else if (args[1].equals("float")) {
				float i = Float.parseFloat(args[3]);
				System.out.println("writing float \"" + i + "\" to " + args[2]);
				writeFloat(fs, args[2], i);
			} else if (args[1].equals("long")) {
				long i = Long.parseLong(args[3]);
				System.out.println("writing long \"" + i + "\" to " + args[2]);
				writeLong(fs, args[2], i);
			} else if (args[1].equals("String")) {
				System.out.println("writing String \"" + args[3] + "\" to " + args[2]);
				writeString(fs, args[2], args[3]);
			} else {
				System.out.println("unknown write type");
				System.out.println("args: write (int|long|float|String) [file] [value]");
				System.exit(-1);
			}
		}
	}
}
