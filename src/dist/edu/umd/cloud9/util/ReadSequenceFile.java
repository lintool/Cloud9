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

package edu.umd.cloud9.util;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;

/**
 * <p>
 * Program that reads either a SequenceFile or a directory containing
 * SequenceFiles. A maximum number of key-value pairs to read must be specified;
 * in the of a directory, the value specifies the number of key-value pairs to
 * read <i>per file</i>.
 * </p>
 * 
 * <pre>
 * args: [path] [max-num-of-records]
 * </pre>
 */
public class ReadSequenceFile {

	private ReadSequenceFile() {
	}
	
	public static void main(String[] args) throws IOException {
		if (args.length < 1) {
			System.out.println("args: [path] [max-num-of-records-per-file]");
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

		if (fileSys.getFileStatus(p).isDir())
			readSequenceFilesInDir(p, max);
		else
			readSequenceFile(p, max);
	}

	private static int readSequenceFile(Path path, int max) throws IOException {
		Configuration config = new Configuration();
		SequenceFile.Reader reader = new SequenceFile.Reader(FileSystem.get(config), path, config);

		System.out.println("Reading " + path + "...\n");
		try {
			System.out.println("Key type: " + reader.getKeyClass().toString());
			System.out.println("Value type: " + reader.getValueClass().toString() + "\n");
		} catch (Exception e) {
			throw new RuntimeException("Error: loading key/value class");
		}

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

				if (n >= max)
					break;
			}
			reader.close();
			System.out.println(n + " records read.\n");
		} catch (Exception e) {
			e.printStackTrace();
		}

		return n;
	}

	private static int readSequenceFilesInDir(Path path, int max) {
		JobConf config = new JobConf();
		int n = 0;
		try {
			FileSystem fileSys = FileSystem.get(config);
			FileStatus[] stat = fileSys.listStatus(path);
			for (int i = 0; i < stat.length; ++i) {
				n += readSequenceFile(stat[i].getPath(), max);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}

		System.out.println(n + " records read.");
		return n;
	}

}
