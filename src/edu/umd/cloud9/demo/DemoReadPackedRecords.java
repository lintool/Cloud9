package edu.umd.cloud9.demo;

import java.io.IOException;

import edu.umd.cloud9.tuple.LocalTupleRecordReader;
import edu.umd.cloud9.tuple.Tuple;

public class DemoReadPackedRecords {

	private static Tuple tuple = new Tuple();

	public static void main(String[] args) throws IOException {
		String file = "../umd-hadoop-dist/sample-input/bible+shakes.nopunc.packed";
		
		LocalTupleRecordReader reader = new LocalTupleRecordReader(file);
		while ( reader.read(tuple) ) {
			System.out.println(tuple.get(0));
		}
		reader.close();
		
		System.out.println("Read " + reader.getRecordCount() + " records.");
	}

}
