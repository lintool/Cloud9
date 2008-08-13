package edu.umd.cloud9.util;

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

public class PackTextFile {

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

		SequenceFile.Writer writer = SequenceFile.createWriter(FileSystem
				.get(config), config, new Path(outFile), LongWritable.class,
				Text.class);

		BufferedReader reader = new BufferedReader(new FileReader(new File(
				inFile)));

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
