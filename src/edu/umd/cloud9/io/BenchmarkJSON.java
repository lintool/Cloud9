package edu.umd.cloud9.io;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.io.WritableComparable;
import org.json.JSONException;

public class BenchmarkJSON {

	private static class MyJSONTuple extends JSONObjectWritable implements WritableComparable {
		public int compareTo(Object obj) {
			try {
				MyJSONTuple that = (MyJSONTuple) obj;

				int thisLeft = this.getIntUnchecked("left");
				int thisRight = this.getIntUnchecked("right");

				int thatLeft = that.getIntUnchecked("left");
				int thatRight = that.getIntUnchecked("right");

				if (thisLeft == thatLeft) {
					if (thisRight < thatRight)
						return -1;

					if (thisRight > thatRight)
						return 1;

					return 0;
				}

				if (thisLeft < thatLeft)
					return -1;

				if (thisLeft > thatLeft)
					return 1;

				return 0;
			} catch (JSONException e) {
				e.printStackTrace();
				throw new RuntimeException("Unexpected error comparing JSON objects!");
			}
		}
	}

	@SuppressWarnings("unchecked")
	public static void main(String[] args) throws Exception {
		Random r = new Random();

		long startTime;
		double duration;

		startTime = System.currentTimeMillis();
		List<MyJSONTuple> listJSONObjects1 = new ArrayList<MyJSONTuple>();
		for (int i = 0; i < 2000000; i++) {
			MyJSONTuple j = new MyJSONTuple();
			j.put("left", r.nextInt(1000));
			j.put("right", r.nextInt(1000));
			listJSONObjects1.add(j);
		}

		duration = (System.currentTimeMillis() - startTime) / 1000.0;
		System.out.println("Generated 2m JSON Objects in " + duration + " seconds");

		startTime = System.currentTimeMillis();
		List<MyJSONTuple> listJSONObjects2 = new ArrayList<MyJSONTuple>();
		for (MyJSONTuple t : listJSONObjects1) {
			MyJSONTuple n = new MyJSONTuple();
			n.put("left", t.getInt("left"));
			n.put("right", t.getInt("right"));
			listJSONObjects2.add(n);
		}

		duration = (System.currentTimeMillis() - startTime) / 1000.0;
		System.out.println("Cloned 2m JSON Objects in " + duration + " seconds");

		startTime = System.currentTimeMillis();
		Collections.sort(listJSONObjects2);
		duration = (System.currentTimeMillis() - startTime) / 1000.0;

		System.out.println("Sorted 2m JSON Objects in " + duration + " seconds");
	}
}
