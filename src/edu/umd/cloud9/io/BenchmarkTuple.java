package edu.umd.cloud9.io;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

public class BenchmarkTuple {

	// create the schema for the tuple that will serve as the key
	private static final Schema MY_SCHEMA = new Schema();

	// define the schema statically
	static {
		MY_SCHEMA.addField("left", Integer.class, new Integer(1));
		MY_SCHEMA.addField("right", Integer.class, new Integer(1));
	}

	@SuppressWarnings("unchecked")
	public static void main(String[] args) throws Exception {
		Random r = new Random();

		long startTime;
		double duration;

		startTime = System.currentTimeMillis();

		List<Tuple> listTuples1 = new ArrayList<Tuple>();
		for (int i = 0; i < 2000000; i++) {
			Tuple tuple = MY_SCHEMA.instantiate();
			tuple.set(0, r.nextInt(1000));
			tuple.set(1, r.nextInt(1000));
			listTuples1.add(tuple);
		}

		duration = (System.currentTimeMillis() - startTime) / 1000.0;
		System.out.println("Generated 2m Tuples in " + duration + " seconds");

		startTime = System.currentTimeMillis();

		List<Tuple> listTuples2 = new ArrayList<Tuple>();
		for (Tuple t : listTuples1) {
			Tuple n = MY_SCHEMA.instantiate();
			n.set(0, t.get(0));
			n.set(1, t.get(1));
			listTuples2.add(n);
		}

		duration = (System.currentTimeMillis() - startTime) / 1000.0;
		System.out.println("Cloned 2m Tuples in " + duration + " seconds");

		startTime = System.currentTimeMillis();
		Collections.sort(listTuples2);
		duration = (System.currentTimeMillis() - startTime) / 1000.0;

		System.out.println("Sorted 2m Tuples in " + duration + " seconds");

	}
}
