package edu.umd.hooka.alignment;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

import junit.framework.TestCase;
import edu.umd.hooka.ttables.TTable_monolithic_IFAs;

public class IndexedFloatArrayTest  extends TestCase {

	
	public void testSetArray() {
		int[] indices = {1,4,6,16};
		float[] probs = {0.1f,0.2f,0.3f,0.4f};
		
		TTable_monolithic_IFAs table = new TTable_monolithic_IFAs();
		table.set(7, new IndexedFloatArray(indices, probs, false));
		
		assertTrue(table.get(7,4)==0.2f);
		assertTrue(table.get(7,6)==0.3f);
		assertTrue(table.get(7,1)==0.1f);
		assertTrue(table.get(7,16)==0.4f);
	}
		
	
	public void testReadFields() {
		try {
		File temp = File.createTempFile("fat", null);
		temp.deleteOnExit();
		DataOutputStream dos = new DataOutputStream(
			new FileOutputStream(temp));
		int[] i = {1, 4, 8, 9, 10};
		IndexedFloatArray fa = new IndexedFloatArray(i);
		IndexedFloatArray fa2 = new IndexedFloatArray(10);
		fa.set(1, 0.5f);
		fa.set(10, 1.0f);
		fa.add(8, 0.1f);
		fa.add(8, 0.4f);
		fa2.set(4, 2.0f);
		fa.write(dos);
		fa2.write(dos);
		dos.close();
		DataInputStream dis = new DataInputStream(
				new FileInputStream(temp));
		fa2 = new IndexedFloatArray();
		IndexedFloatArray fa3 = new IndexedFloatArray();
		fa2.readFields(dis);
		fa3.readFields(dis);
		assertEquals(0.5f, fa2.get(1));
		assertEquals(0.5f, fa2.get(8));
		assertEquals(1.0f, fa2.get(10));
		System.err.println(fa2);
		assertEquals(2.0f, fa3.get(4));
		
		} catch (IOException e) {
			fail("Caught " + e);
		}
	}
	
	public void testAdd() {
		IndexedFloatArray acc = new IndexedFloatArray();
		int[] e = {1, 4, 10, 99};
		int[] e2= {1, 2, 3, 4};
		IndexedFloatArray v1 = new IndexedFloatArray(e);
		IndexedFloatArray v2 = new IndexedFloatArray(e2);
		for (int i : e) v1.set(i, 1.0f);
		for (int i : e2) v2.set(i, 1.0f);
		acc.plusEqualsMismatchSize(v1);
		acc.plusEqualsMismatchSize(v2);
		assertEquals(2.0f, acc.get(1));
		System.out.println("TA: " + acc);
		assertEquals(1.0f, acc.get(99));
		assertEquals(2.0f, acc.get(4));
	}
	
	public void testIndexedFloatArray() {
		int[] i = {1, 4, 8, 9, 10};
		IndexedFloatArray fa = new IndexedFloatArray(i);
		fa.set(1, 0.5f);
		fa.set(10, 1.0f);
		fa.add(8, 0.1f);
		fa.add(8, 0.4f);
		assertEquals(0.5f, fa.get(1));
		assertEquals(0.5f, fa.get(8));
		assertEquals(1.0f, fa.get(10));
		try {
			fa.set(2, 1.0f);
			fail("Should throw!");
		} catch (RuntimeException r) {}
		System.err.println(fa);
	}
	
	public void testDense() {
		IndexedFloatArray x = new IndexedFloatArray(5);
		x.set(1, 0.4f);
		x.set(2, 0.5f);
		assertEquals(0.4f, x.get(1));
		assertEquals(0.5f, x.get(2));
		System.err.println(x);
		IndexedFloatArray y = (IndexedFloatArray)x.clone();
		x.plusEquals(y);
		System.err.println(y);
		System.err.println(x);
		assertEquals(0.8f, x.get(1));
		try {
			File temp = File.createTempFile("fat", null);
			temp.deleteOnExit();
			DataOutputStream dos = new DataOutputStream(
					new FileOutputStream(temp));
			x.write(dos);
			dos.close();
			DataInputStream dis = new DataInputStream(
					new FileInputStream(temp));
			IndexedFloatArray fa2 = new IndexedFloatArray();
			fa2.readFields(dis);
			assertEquals(0.8f, fa2.get(1));
			assertEquals(x.size(), fa2.size());
		} catch (IOException e) { fail("Caught " + e); }
	}
	
	public void testPlusEquals() {
		int[] i = {1, 4, 5};
		IndexedFloatArray x = new IndexedFloatArray(i);
		IndexedFloatArray y = new IndexedFloatArray(i);
		x.set(1, 0.2f);
		x.set(4, 0.1f);
		y.set(4, 0.4f);
		y.set(5, 1.0f);
		x.plusEquals(y);
		assertEquals(0.2f, x.get(1));
		assertEquals(0.5f, x.get(4));
		assertEquals(1.0f, x.get(5));
	}

	public void testPlusEqualsMML() {
		int[] i = {1, 14, 15};
		int[] j = {14, 23};
		IndexedFloatArray x = new IndexedFloatArray(i);
		IndexedFloatArray y = new IndexedFloatArray(j);
		x.set(1, 0.2f);
		x.set(14, 0.1f);
		x.set(15, 1.0f);
		y.set(14, 0.4f);
		y.set(23, 0.8f);
		y.plusEqualsMismatchSize(x);
		System.out.println(y);
		assertEquals(0.2f, y.get(1));
		assertEquals(0.5f, y.get(14));
		assertEquals(1.0f, y.get(15));
		assertEquals(0.8f, y.get(23));
		
		IndexedFloatArray z = new IndexedFloatArray(3);
		z.set(0, 1.0f);
		z.set(1, 1.0f);
		z.set(2, 1.0f);
		y.plusEqualsMismatchSize(z);
		assertEquals(1.0f, y.get(0));
		assertEquals(1.2f, y.get(1));
		assertEquals(1.0f, y.get(2));
		assertEquals(0.5f, y.get(14));
		assertEquals(1.0f, y.get(15));
		assertEquals(0.8f, y.get(23));
		z.plusEqualsMismatchSize(y);
		assertEquals(2.0f, z.get(0));
		assertEquals(2.2f, z.get(1));
		assertEquals(2.0f, z.get(2));
		assertEquals(0.5f, z.get(14));
		assertEquals(1.0f, z.get(15));
		assertEquals(0.8f, z.get(23));
	}
	
	public void testInit() {
		float[] v = { 1.0f, 2.0f, 3.0f, 5.0f, 5.0f,
				      1.0f, 2.0f, 3.0f, 0.2f, 5.0f,
				      1.0f, 2.0f, 3.0f, 0.0f, 0.4f};
		IndexedFloatArray a = new IndexedFloatArray(v, v.length);
		System.out.println(a);
	}

	public void testNormalize() {
		int[] i = {1, 4, 5};
		IndexedFloatArray x = new IndexedFloatArray(i);
		x.set(1, 0.1f);
		x.set(4, 0.3f);
		x.set(5, 0.4f);
		x.normalize();
		System.err.println(x);
		assertEquals(0.125f, x.get(1));
		assertEquals(0.375f, x.get(4));
		assertEquals(0.5f, x.get(5));
	}
}
