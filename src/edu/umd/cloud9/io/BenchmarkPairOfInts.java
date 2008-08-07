package edu.umd.cloud9.io;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

public class BenchmarkPairOfInts {

	@SuppressWarnings("unchecked")
	public static void main(String[] args) throws Exception {
		Random r = new Random();

		long startTime;
		double duration;

		startTime = System.currentTimeMillis();

		List<PairOfInts> listPairOfInts1 = new ArrayList<PairOfInts>();
		for (int i = 0; i < 2000000; i++) {
			listPairOfInts1.add(new PairOfInts(r.nextInt(1000), r.nextInt(1000)));
		}

		duration = (System.currentTimeMillis() - startTime) / 1000.0;
		System.out.println("Generated 2m PairOfInts in " + duration + " seconds");

		startTime = System.currentTimeMillis();
		List<PairOfInts> listPairOfInts2 = new ArrayList<PairOfInts>();
		for (PairOfInts p : listPairOfInts1) {
			listPairOfInts2.add(p.clone());
		}

		duration = (System.currentTimeMillis() - startTime) / 1000.0;
		System.out.println("Cloned 2m PairOfInts in " + duration + " seconds");

		startTime = System.currentTimeMillis();
		Collections.sort(listPairOfInts2);
		duration = (System.currentTimeMillis() - startTime) / 1000.0;

		System.out.println("Sorted 2m PairOfInts in " + duration + " seconds");
	}
}
