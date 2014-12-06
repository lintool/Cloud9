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

package edu.umd.cloud9.io;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;

/**
 * <p>
 * Program that takes a plain text file and converts it into a SequenceFile. The
 * key is a LongWritable that sequentially counts the line number. The value is
 * a Text containing each line, without a trailing newline.
 * </p>
 *
 * <pre>
 * args: [input-file] [output-file]
 * </pre>
 */
public class PackTextFile {

	private PackTextFile() {}

	public static void main(String[] args) throws IOException {

		if (args.length < 2) {
			System.err.println("args: [input-file] [output-file]");
			System.exit(-1);
		}

		String inFile = args[0];
		String outFile = args[1];

		Text text = new Text();
		LongWritable lw = new LongWritable();
		long l = 0;

		JobConf config = new JobConf();

		SequenceFile.Writer writer = SequenceFile.createWriter(FileSystem.get(config), config,
				new Path(outFile), LongWritable.class, Text.class);

		BufferedReader reader = new BufferedReader(new FileReader(new File(inFile)));

		String line = "";
		while ((line = reader.readLine()) != null) {
			lw.set(l);
			text.set(line);
			writer.append(lw, text);
			l++;
		}

		reader.close();
		writer.close();

		System.out.println("Wrote a total of " + l + " records");
	}

}
