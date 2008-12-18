/*
 * The contents of this file are subject to the terms of the Common Development
 * and Distribution License (the License). You may not use this file except in
 * compliance with the License.
 *
 * You can obtain a copy of the License at http://www.netbeans.org/cddl.html
 * or http://www.netbeans.org/cddl.txt.
 *
 * When distributing Covered Code, include this CDDL Header Notice in each file
 * and include the License file at http://www.netbeans.org/cddl.txt.
 * If applicable, add the following below the CDDL Header, with the fields
 * enclosed by brackets [] replaced by your own identifying information:
 * "Portions Copyrighted [year] [name of copyright owner]"
 *
 * The Original Software is GraphMaker. The Initial Developer of the Original
 * Software is Nathan L. Fiedler. Portions created by Nathan L. Fiedler
 * are Copyright (C) 2007-2008. All Rights Reserved.
 *
 * Contributor(s): Nathan L. Fiedler.
 *
 * $Id: FibonacciHeapTest.java 19 2008-12-04 20:28:06Z nathanfiedler $
 */

package edu.umd.cloud9.util;

import java.util.Hashtable;
import java.util.Random;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import edu.umd.cloud9.util.FibonacciHeapInt.Node;

/**
 * Tests the FibonacciHeap class.
 * 
 * @author Nathan Fiedler
 */
public class FibonacciHeapIntTest extends TestCase {

	public FibonacciHeapIntTest(String name) {
		super(name);
	}

	public static Test suite() {
		return new TestSuite(FibonacciHeapIntTest.class);
	}

	public static void main(String[] args) {
		junit.textui.TestRunner.run(suite());
	}

	/**
	 * Inserts a set of elements, decreases some of their keys, then extracts
	 * them by key order and ensures everything comes out in the order expected.
	 */
	public void test_Correctness() {
		FibonacciHeapInt heap = new FibonacciHeapInt();
		assertTrue(heap.isEmpty());
		assertEquals(0, heap.size());
		Hashtable<Integer, FibonacciHeapInt.Node> entries = new Hashtable<Integer, FibonacciHeapInt.Node>();
		for (int ii = 100; ii < 200; ii++) {
			Integer it = new Integer(ii);
			entries.put(it, heap.insert(it, ii));
		}
		assertFalse(heap.isEmpty());
		assertEquals(100, heap.size());
		FibonacciHeapInt.Node entry = entries.get(new Integer(110));
		heap.decreaseKey(entry, 50);
		entry = entries.get(new Integer(140));
		heap.decreaseKey(entry, 25);
		entry = entries.get(new Integer(160));
		heap.decreaseKey(entry, 15);
		// Last one should be the min value.
		assertEquals(entry, heap.min());
		int o = heap.removeMin().getDatum();
		assertEquals(160, o);
		// Second last should now be the min value.
		entry = entries.get(new Integer(140));
		assertEquals(entry, heap.min());
		heap.delete(entry);
		// Remove the third smallest entry.
		entry = entries.get(new Integer(110));
		heap.delete(entry);
		// Original min value should now be the min.
		entry = entries.get(new Integer(100));
		assertEquals(entry, heap.min());
		heap.clear();
		assertTrue(heap.isEmpty());
		assertEquals(0, heap.size());
	}

	/**
	 * Test a heap consisting of all duplicate keys.
	 */
	public void test_Duplicates() {
		FibonacciHeapInt heap = new FibonacciHeapInt();
		assertTrue(heap.isEmpty());
		assertEquals(0, heap.size());
		// Insert entries with duplicate keys.
		float key = Float.MIN_NORMAL;
		for (int ii = 1; ii < 1001; ii++) {
			Integer it = new Integer(ii);
			heap.insert(it, key);
		}
		assertFalse(heap.isEmpty());
		assertEquals(1000, heap.size());
		Node o = heap.removeMin();
		assertTrue(o instanceof Node);
		assertFalse(heap.isEmpty());
		assertEquals(999, heap.size());
		heap.clear();
		assertTrue(heap.isEmpty());
		assertEquals(0, heap.size());
	}

	/**
	 * Test a heap consisting of all duplicate keys, except for one whose value
	 * is greater than the others.
	 */
	public void test_Duplicates_Larger() {
		FibonacciHeapInt heap = new FibonacciHeapInt();
		assertTrue(heap.isEmpty());
		assertEquals(0, heap.size());
		// Insert entries with duplicate keys.
		float key = 0.0f;
		for (int ii = 1; ii < 1000; ii++) {
			Integer it = new Integer(ii);
			heap.insert(it, key);
		}
		heap.insert(new Integer(1001), Float.MIN_NORMAL);
		assertFalse(heap.isEmpty());
		assertEquals(1000, heap.size());
		Node o = heap.removeMin();
		assertTrue(o instanceof Node);
		assertTrue(o.getDatum() < 1001);
		assertFalse(heap.isEmpty());
		assertEquals(999, heap.size());
		heap.clear();
		assertTrue(heap.isEmpty());
		assertEquals(0, heap.size());
	}

	/**
	 * Test a heap consisting of all duplicate keys, except for one whose value
	 * is less than the others.
	 */
	public void test_Duplicates_Smaller() {
		FibonacciHeapInt heap = new FibonacciHeapInt();
		assertTrue(heap.isEmpty());
		assertEquals(0, heap.size());
		// Insert entries with duplicate keys.
		float key = Float.MIN_NORMAL;
		for (int ii = 1; ii < 1000; ii++) {
			Integer it = new Integer(ii);
			heap.insert(it, key);
		}
		heap.insert(new Integer(1001), 0.0f);
		assertFalse(heap.isEmpty());
		assertEquals(1000, heap.size());
		Node o = heap.removeMin();
		assertTrue(o instanceof Node);
		assertTrue(o.getDatum() == 1001);
		assertFalse(heap.isEmpty());
		assertEquals(999, heap.size());
		heap.clear();
		assertTrue(heap.isEmpty());
		assertEquals(0, heap.size());
	}

	/**
	 * This is a stress test that inserts numerous random elements and ensures
	 * that they come out in increasing order by value. This extreme case
	 * uncovered multiple bugs in nearly every public implementation of
	 * fibonacci heap.
	 */
	public void test_InsertRemoveMin() {
		FibonacciHeapInt heap = new FibonacciHeapInt();
		assertTrue(heap.isEmpty());
		assertEquals(0, heap.size());

		// See comment in FibonacciHeapTest.

		// Insert a lot of random numbers.
		Random random = new Random();
		for (int ii = 1; ii <= 50000; ii++) {
			float r = random.nextFloat();
			if (r < 0)
				continue;
			heap.insert((int) r, r);
		}
		assertEquals(50000, heap.size());
		// Ensure the numbers come out in increasing order.
		float ii = 0.0f;
		int count = 0;
		while (!heap.isEmpty()) {
			int v = heap.removeMin().getDatum();
			count++;
			assertTrue(v >= ii);
			ii = v;
		}
		// Ensure no elements were lost on the way out.
		assertEquals(50000, count);
		assertTrue(heap.isEmpty());
		assertEquals(0, heap.size());
	}

	public void test_Union() {
		FibonacciHeapInt heap1 = new FibonacciHeapInt();
		assertTrue(heap1.isEmpty());
		assertEquals(0, heap1.size());
		heap1.insert(new Integer(1), 1);
		heap1.insert(new Integer(2), 2);
		heap1.insert(new Integer(3), 3);
		heap1.insert(new Integer(4), 4);
		heap1.insert(new Integer(5), 5);
		assertFalse(heap1.isEmpty());
		assertEquals(5, heap1.size());
		FibonacciHeapInt heap2 = new FibonacciHeapInt();
		assertTrue(heap2.isEmpty());
		assertEquals(0, heap2.size());
		heap2.insert(new Integer(6), 6);
		heap2.insert(new Integer(7), 7);
		heap2.insert(new Integer(8), 8);
		heap2.insert(new Integer(9), 9);
		heap2.insert(new Integer(10), 10);
		assertFalse(heap2.isEmpty());
		assertEquals(5, heap2.size());
		FibonacciHeapInt joined = FibonacciHeapInt.union(heap1, heap2);
		assertFalse(joined.isEmpty());
		assertEquals(10, joined.size());
		Integer v = (Integer) joined.removeMin().getDatum();
		int vi = v.intValue();
		int ii = 1;
		assertTrue(vi == ii);
		while (!joined.isEmpty()) {
			v = (Integer) joined.removeMin().getDatum();
			vi = v.intValue();
			assertTrue(vi > ii);
			ii = vi;
		}
		assertTrue(joined.isEmpty());
		assertEquals(0, joined.size());
	}

	public void test1() {
		FibonacciHeapInt heap = new FibonacciHeapInt();
		
		heap.insert(1, 3.1f);
		heap.insert(2, 4.6f);
		heap.insert(3, 2.0f);
		heap.insert(4, 2.9f);

		assertEquals(3, heap.removeMin().getDatum());
		assertEquals(4, heap.removeMin().getDatum());
		assertEquals(1, heap.removeMin().getDatum());
		assertEquals(2, heap.removeMin().getDatum());
	}

	public void test2() {
		FibonacciHeapInt heap = new FibonacciHeapInt();
		
		heap.insert(1, 3.1f);
		heap.insert(2, 3.1f);
		heap.insert(3, 2.0f);
		heap.insert(4, 4.5f);
		heap.insert(5, 3.1f);
		heap.insert(6, 2.0f);

		assertEquals(3, heap.removeMin().getDatum());
		assertEquals(6, heap.removeMin().getDatum());
		assertEquals(1, heap.removeMin().getDatum());
		assertEquals(2, heap.removeMin().getDatum());
		assertEquals(5, heap.removeMin().getDatum());
		assertEquals(4, heap.removeMin().getDatum());
	}
	
}
