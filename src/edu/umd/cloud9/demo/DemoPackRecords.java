package edu.umd.cloud9.demo;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;

import edu.umd.cloud9.tuple.LocalTupleRecordWriter;
import edu.umd.cloud9.tuple.Schema;
import edu.umd.cloud9.tuple.Tuple;

public class DemoPackRecords {

	public static final Schema RECORD_SCHEMA = new Schema();
	static {
		RECORD_SCHEMA.addField("text", String.class, "");
	}

	private static Tuple tuple = RECORD_SCHEMA.instantiate();

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
