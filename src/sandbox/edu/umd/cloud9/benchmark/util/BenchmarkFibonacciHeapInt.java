package edu.umd.cloud9.benchmark.util;

import java.util.Random;

import edu.umd.cloud9.util.FibonacciHeapInt;

public class BenchmarkFibonacciHeapInt {
	public static void main(String[] args) {
		FibonacciHeapInt heap = new FibonacciHeapInt();
		Random r = new Random();

		long startTime;
		long endTime;

		startTime = System.currentTimeMillis();
		for (int i = 0; i < 1000000; i++) {
			int n = r.nextInt(1000);
			float f = r.nextFloat();

			heap.insert(n, f);
		}

		endTime = System.currentTimeMillis();
		System.out.println("inserts completed in " + (endTime - startTime) + " ms");

		long sum = 0;
		startTime = System.currentTimeMillis();
		for (int i = 0; i < 500000; i++) {
			sum += heap.removeMin().getDatum();
		}
		endTime = System.currentTimeMillis();
		System.out.println("mins completed in " + (endTime - startTime) + " ms");
	}
}
