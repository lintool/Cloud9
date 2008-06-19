package edu.umd.cloud9.util;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.conf.Configuration;

public class ReadSequenceFile {

	public static void main(String[] args) throws IOException {
		if (args.length < 1) {
			System.out
					.println("Usage: ReadSequenceFile [file] [max-num-of-records]");
			System.exit(-1);
		}

		String f = args[0];
		
		int max = Integer.MAX_VALUE;
		if (args.length >= 2) {
			max = Integer.parseInt(args[1]);
		}

		Configuration config = new JobConf();
		FileSystem fileSys = FileSystem.get(config);
		Path p = new Path(f);
		if (fileSys.isDirectory(p))
			readSequenceFilesInDir(p, max);
		else
			readSequenceFile(p, max);
	}

	private static int readSequenceFile(Path filename, int max) throws IOException {
		JobConf config = new JobConf();
		SequenceFile.Reader reader = new SequenceFile.Reader(FileSystem
				.get(config), filename, config);

		System.out.println("Reading " + filename + "...\n");
		System.out.println("Key type: " + reader.getKeyClass().toString());
		System.out.println("Value type: " + reader.getValueClass().toString() + "\n");

		Writable key, value;
		int n = 0;
		try {
			key = (Writable) reader.getKeyClass().newInstance();
			value = (Writable) reader.getValueClass().newInstance();

			while (reader.next(key, value)) {
				System.out.println("Record " + n);
				System.out.println("Key: " + key + "\nValue: " + value);
				System.out.println("----------------------------------------");
				n++;
				
				if ( n >= max)
					break;
			}
			reader.close();
			System.out.println(n + " records read.\n");
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		return n;
	}

	private static int readSequenceFilesInDir(Path inPath, int max) {
		JobConf config = new JobConf();
		int n = 0;
		try {
			FileSystem fileSys = FileSystem.get(config);
			Path[] files = fileSys.listPaths(inPath);
			for (int i = 0; i < files.length; i++) {
				n += readSequenceFile(files[i], max);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		System.out.println(n + " records read.");
		return n;
	}

}
