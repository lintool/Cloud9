package edu.umd.cloud9.tuple;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapred.JobConf;

public class PackRecordsDemo {


	public static final Schema RECORD_SCHEMA = new Schema();
	static {
		RECORD_SCHEMA.addField("text", String.class, "");
	}

	public static Tuple tuple = RECORD_SCHEMA.instantiate();

	public static void main(String[] args) throws IOException {
		String infile = "../umd-hadoop-dist/sample-input/bible+shakes.nopunc";
		String outfile = "../umd-hadoop-dist/sample-input/bible+shakes.nopunc.packed";

		JobConf config = new JobConf();
		
		System.out.println(config.getWorkingDirectory());
		
		BytesWritable bytes = new BytesWritable();
		LongWritable longw = new LongWritable();
		
		SequenceFile.Writer writer = SequenceFile.createWriter(FileSystem.get(config), config,
				new Path(outfile), LongWritable.class, BytesWritable.class);

			BufferedReader data = new BufferedReader(new InputStreamReader(
					new FileInputStream(infile)));

			String line;
			long cnt = 0;
			while ((line = data.readLine()) != null) {
				tuple.set(0, line);
				byte[] buf = tuple.pack();
				
				longw.set(cnt);
				bytes.set(buf, 0, buf.length);
				writer.append(longw, bytes);
				cnt++;
			}

			data.close();
			writer.close();

			System.out.println("Wrote " + cnt + " records.");

	}
	
	/*
	public static void main(String[] args) throws IOException {

		FileSystem fs = new RawLocalFileSystem();
		JobConf config = new JobConf();
		
		System.out.println(config.getWorkingDirectory());
		
		SequenceFile.Writer writer = SequenceFile.createWriter(FileSystem.get(config), config,
				new Path("test.out"), LongWritable.class, BytesWritable.class);

		writer.append(new LongWritable(1), new BytesWritable());
		
		writer.close();
	}*/
}
