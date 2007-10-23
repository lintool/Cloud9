package edu.umd.cloud9.tuple;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;

public class PackRecordsDemo {

	public static final Schema RECORD_SCHEMA = new Schema();
	static {
		RECORD_SCHEMA.addField("text", String.class, "");
	}

	public static Tuple tuple = RECORD_SCHEMA.instantiate();

	public static void main(String[] args) throws IOException {
		String infile = "../umd-hadoop-dist/sample-input/bible+shakes.nopunc";
		String outfile = "../umd-hadoop-dist/sample-input/bible+shakes.nopunc.packed";

		LocalTupleRecordWriter writer = new LocalTupleRecordWriter(outfile);

		BufferedReader data = new BufferedReader(new InputStreamReader(
				new FileInputStream(infile)));

		String line;
		while ((line = data.readLine()) != null) {
			tuple.set(0, line);
			writer.add(tuple);
		}

		data.close();
		writer.close();

		System.out.println("Wrote " + writer.getRecordCount() + " records.");

	}

}
