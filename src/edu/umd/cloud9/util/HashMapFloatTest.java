package edu.umd.cloud9.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Random;

import junit.framework.JUnit4TestAdapter;

import org.junit.Test;

public class HashMapFloatTest {

	@Test
	public void testBasic1() {
		int size = 100000;
		Random r = new Random();
		float[] floats = new float[size];

		MapFloat<Integer> map = new HashMapFloat<Integer>();
		for (int i = 0; i < size; i++) {
			int k = r.nextInt(size);
			map.put(i, k + 0.1f);
			floats[i] = k + 0.1f;
		}

		for (int i = 0; i < size; i++) {
			float v = map.get(i);

			assertEquals(floats[i], v, 0.0f);
			assertTrue(map.containsKey(i));
		}
	}

	@Test
	public void testBasic2() {
		int size = 100000;
		Random r = new Random();
		float[] floats = new float[size];
		String[] strings = new String[size];

		MapFloat<String> map = new HashMapFloat<String>();
		for (int i = 0; i < size; i++) {
			int k = r.nextInt(size);
			String s = new Integer(k).toString();
			map.put(s, k + 0.1f);
			floats[i] = k + 0.1f;
			strings[i] = s;
		}

		for (int i = 0; i < size; i++) {
			float v = map.get(strings[i]);

			assertEquals(floats[i], v, 0.0f);
			assertTrue(map.containsKey(strings[i]));
		}
	}

	@Test
	public void testUpdate() {
		int size = 100000;
		Random r = new Random();
		float[] floats = new float[size];

		MapFloat<Integer> map = new HashMapFloat<Integer>();
		for (int i = 0; i < size; i++) {
			int k = r.nextInt(size);
			map.put(i, k + 0.1f);
			floats[i] = k + 0.1f;
		}

		assertEquals(size, map.size());

		for (int i = 0; i < size; i++) {
			map.put(i, floats[i] + 1.0f);
		}

		assertEquals(size, map.size());

		for (int i = 0; i < size; i++) {
			float v = map.get(i);

			assertEquals(floats[i] + 1.0f, v, 0.0f);
			assertTrue(map.containsKey(i));
		}

	}

	public static junit.framework.Test suite() {
		return new JUnit4TestAdapter(HashMapFloatTest.class);
	}

}