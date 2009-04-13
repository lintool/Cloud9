package edu.umd.cloud9.debug;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.Reporter;

public class SimpleMapDebugHarness {

	public static void run(WritableComparable k, Writable v, Class<?> c) {

		InMemoryOutputCollector collector = new InMemoryOutputCollector();

		Mapper<WritableComparable, Writable, WritableComparable, Writable> mapper = null;
		try {
			mapper = (Mapper<WritableComparable, Writable, WritableComparable, Writable>) c
					.newInstance();
		} catch (Exception e) {
			e.printStackTrace();
		}

		try {
			mapper.map(k, v, collector, Reporter.NULL);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
