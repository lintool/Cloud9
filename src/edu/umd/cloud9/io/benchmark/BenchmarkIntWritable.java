package edu.umd.cloud9.io.benchmark;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.io.IntWritable;

import edu.umd.cloud9.io.Int3Writable;

public class BenchmarkIntWritable {

	private BenchmarkIntWritable() {
	}

	/**
	 * Runs this benchmark.
	 */
	@SuppressWarnings("unchecked")
	public static void main(String[] args) throws Exception {
		Random r = new Random();

		long startTime;
		double duration;

		startTime = System.currentTimeMillis();

		List<Int3Writable> listInts3Writables1 = new ArrayList<Int3Writable>();
		for (int i = 0; i < 2000000; i++) {
			listInts3Writables1.add(new Int3Writable(r.nextInt(10000)));
		}

		duration = (System.currentTimeMillis() - startTime) / 1000.0;
		System.out.println("Generated 2m Int3Writables in " + duration + " seconds");

		startTime = System.currentTimeMillis();
		List<Int3Writable> listInt3Writables2 = new ArrayList<Int3Writable>();
		for (Int3Writable p : listInts3Writables1) {
			listInt3Writables2.add(p.clone());
		}

		duration = (System.currentTimeMillis() - startTime) / 1000.0;
		System.out.println("Cloned 2m Int3Writables in " + duration + " seconds");

		startTime = System.currentTimeMillis();
		Collections.sort(listInt3Writables2);
		duration = (System.currentTimeMillis() - startTime) / 1000.0;

		System.out.println("Sorted 2m Int3Writables in " + duration + " seconds");

		//
		startTime = System.currentTimeMillis();
		List<IntWritable> listIntsWritables1 = new ArrayList<IntWritable>();
		for (int i = 0; i < 2000000; i++) {
			listIntsWritables1.add(new IntWritable(r.nextInt(10000)));
		}

		duration = (System.currentTimeMillis() - startTime) / 1000.0;
		System.out.println("Generated 2m IntWritables in " + duration + " seconds");

		startTime = System.currentTimeMillis();
		List<IntWritable> listIntWritables2 = new ArrayList<IntWritable>();
		for (IntWritable p : listIntsWritables1) {
			listIntWritables2.add(new IntWritable(p.get()));
		}

		duration = (System.currentTimeMillis() - startTime) / 1000.0;
		System.out.println("Cloned 2m IntWritables in " + duration + " seconds");

		startTime = System.currentTimeMillis();
		Collections.sort(listIntWritables2);
		duration = (System.currentTimeMillis() - startTime) / 1000.0;

		System.out.println("Sorted 2m IntWritables in " + duration + " seconds");
	}
}
