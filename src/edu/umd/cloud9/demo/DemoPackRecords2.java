package edu.umd.cloud9.demo;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.StringTokenizer;

import org.apache.hadoop.io.Text;

import edu.umd.cloud9.tuple.ListWritable;
import edu.umd.cloud9.tuple.Schema;
import edu.umd.cloud9.tuple.Tuple;
import edu.umd.cloud9.util.LocalTupleRecordWriter;

public class DemoPackRecords2 {
	private DemoPackRecords2() {
	}

	// define the tuple schema for the input record
	private static final Schema RECORD_SCHEMA = new Schema();
	static {
		RECORD_SCHEMA.addField("length", Integer.class);
		RECORD_SCHEMA.addField("tokens", ListWritable.class, "");
	}

	// instantiate a single tuple
	private static Tuple tuple = RECORD_SCHEMA.instantiate();

	public static void main(String[] args) throws IOException {
		String infile = "../umd-hadoop-dist/sample-input/bible+shakes.nopunc";
		String outfile = "../umd-hadoop-dist/sample-input/bible+shakes.nopunc.packed2";

		// create LocalTupleRecordWriter to write tuples to a local SequenceFile
		LocalTupleRecordWriter writer = new LocalTupleRecordWriter(outfile);

		// read in raw text records, line separated
		BufferedReader data = new BufferedReader(new InputStreamReader(
				new FileInputStream(infile)));

		String line;
		while ((line = data.readLine()) != null) {
			ListWritable<Text> tokens = new ListWritable<Text>();
			StringTokenizer itr = new StringTokenizer(line);
			while (itr.hasMoreTokens()) {
				tokens.add(new Text(itr.nextToken()));
			}
			
			// write the record
			tuple.set("length", line.length());
			tuple.set("tokens", tokens);
			writer.add(tuple);
		}

		data.close();
		writer.close();

		System.out.println("Wrote " + writer.getRecordCount() + " records.");
	}
}