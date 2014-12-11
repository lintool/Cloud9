package edu.umd.hooka;


import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileInputStream;
import java.util.Iterator;

import edu.umd.hooka.alignment.aer.ReferenceAlignment;

import junit.framework.TestCase;

public class AlignmentTest extends TestCase {

	Alignment a;
	File temp;
	DataInputStream dis;
	
	public AlignmentTest()
	{
		super();
		a = new Alignment(6, 5);
		a.align(0, 0);
		a.align(2, 1);
		a.align(1, 2);
		a.align(5, 2);
		a.align(3, 3);
		a.align(4, 4);		
	}
	
	protected void createFiles() throws Exception {
		super.setUp();
		// gestern habe ich eine Kuh gesehen
		// yesterday i  saw  a   cow  
		File temp = File.createTempFile("align", null);
		temp.deleteOnExit();
		DataOutputStream dos = new DataOutputStream(
			new FileOutputStream(temp));
		a.write(dos);
		dos.close();
		dis = new DataInputStream(
				new FileInputStream(temp));
	}

	protected void deleteFiles() throws Exception {
		super.tearDown();
		dis.close();
	}

	public void testReadFields() {
		Alignment b = new Alignment();
		try {
		  createFiles();
		  b.readFields(dis);
		  Alignment c= Alignment.union(a, b);
		  assertTrue(b.equals(a));
		  assertTrue(b.equals(c));
		  Alignment d= Alignment.union(a, b);
		  assertTrue(b.equals(d));
		  deleteFiles();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public void testEmptyAlignment() {
		Alignment c = new Alignment(4,4,"");
		assertFalse(c.aligned(0, 0));
		assertEquals(c.toString(), "");
	}
	
	public void testUnalignF() {
		Alignment b = (Alignment)a.clone();
		b.unalignF(1);
		assertTrue(a.aligned(1, 2));
		assertFalse(b.aligned(1, 2));
	}
	
	public void testAlignment() {
		Alignment c = new Alignment(2,2,"0-1 1-1");
		assertTrue(c.aligned(0, 1));
		assertTrue(c.aligned(1, 1));
		assertFalse(c.aligned(0, 0));
		assertFalse(c.aligned(1, 0));
		try {
			Alignment err = new Alignment(2,2,"1-2");
			System.err.println(err);
			assertTrue(false);
		} catch (Exception e) {
			assertTrue(true);
		}
	}

	public void testNeighborAligned() {
		assertFalse(a.neighborAligned(0, 0));
	}

	public void testLneighborAligned() {
		assertTrue(a.neighborAligned(3, 2));
	}

	public void testIterator() {
		Alignment b = new Alignment(10,10);
		assertFalse(b.iterator().hasNext());
		
		int sum = 0;
		for (Alignment.IntPair i : a) {
			sum += i.f;
		}
		assertEquals(sum, 15);
		Iterator<Alignment.IntPair> i = a.iterator();
		Alignment.IntPair first = i.next();
		assertEquals(first.f, 0);
		assertEquals(first.e, 0);
		Alignment.IntPair sec = i.next();
		assertEquals(sec.f, 2);
		assertEquals(i.next().f, 1);
		assertEquals(i.next().f, 5);
		assertEquals(i.next().f, 3);
		assertEquals(i.next().e, 4);
		assertFalse(i.hasNext());
	}

	public void testCountNeighbors() {
		assertEquals(a.countNeighbors(0, 1, Alignment.DIAG_NEIGHBORS), 2);
	}

	public void testGetELength() {
		assertEquals(a.getELength(), 5);
	}

	public void testGetFLength() {
		assertEquals(a.getFLength(), 6);
	}

	public void testAligned() {
		assertTrue(a.aligned(0, 0));
	}
	
	public void testMergeEnglishWords() {
		assertTrue(a.aligned(0, 0));
		Alignment x = a.mergeEnglishWords(1,2).mergeEnglishWords(2, 3);
		System.err.println(x.toStringVisual());
	}

	public void testSplitEnglishWords() {
		assertTrue(a.aligned(0, 0));
		Alignment x = a;
		System.err.println(x.toStringVisual());
		x = a.splitEnglishWords(0);
		System.err.println(x.toStringVisual());
	}
	public void testSplitForeignWords() {
		assertTrue(a.aligned(0, 0));
		System.err.println("FOREIGN");
		Alignment x = a;
		System.err.println(x.toStringVisual());
		x = a.splitForeignWords(0);
		System.err.println(x.toStringVisual());
	}

	public void testAlign() {
		Alignment b = (Alignment)a.clone();
		assertTrue(b.equals(a));
		b.align(0, 4);
		assertTrue(b.aligned(0, 4));
		Alignment c = Alignment.intersect(a, b);
		assertTrue(c.equals(a));
		assertFalse(c.equals(b));
	}

	public void testFromGiza() {
		String eline = "a la bruja -ja verde";
		String fline = "NULL ({ 1 }) the ({ 2 }) green ({ 5 }) witch ({ 3 })";
		Alignment b = Alignment.fromGiza(eline, fline, false);
		Alignment c = Alignment.fromGiza(eline, fline, true);
		assertTrue(c.getTranspose().equals(b));
		assertEquals(b.getELength(), 5);
		assertEquals(b.getFLength(), 3);
		assertEquals(c.getELength(), 3);
		assertEquals(c.getFLength(), 5);
		String eline2 = "the green witch";
		String fline2 = "NULL ({ }) a ({ }) la ({ 1 }) bruja ({ 3 }) -ja ({ 3 }) verde ({ 2 })";
		Alignment x = Alignment.fromGiza(eline2, fline2, false);
		Alignment union = Alignment.union(c, x);
		assertFalse(union.equals(c));
	}
	
	public void testGetTranspose() {
		Alignment b = a.getTranspose();
		assertEquals(a.getELength(), b.getFLength());
		assertEquals(b.getELength(), a.getFLength());
	}

	public void testToString() {
		assertEquals(a.toString(), "0-0 1-2 2-1 3-3 4-4 5-2");
	}

	public void testIntersect() {
		Alignment a = new Alignment(2,2);
		Alignment b = new Alignment(2,2);
		a.align(1, 1);
		a.align(0, 0);
		b.align(0, 1);
		b.align(1, 0);
		Alignment c = Alignment.intersect(a, b);
		Alignment d = new Alignment(2,2);
		assertTrue(d.equals(c));
		b.align(0, 0);
		c = Alignment.intersect(a, b);
		assertTrue(c.aligned(0, 0));
		assertFalse(c.aligned(0, 1));
		assertFalse(c.aligned(1, 1));
		assertFalse(c.aligned(1, 0));
	}

	public void testUnion() {
		Alignment a = new Alignment(2,2);
		Alignment b = new Alignment(2,2);
		a.align(1, 1);
		a.align(0, 0);
		b.align(0, 1);
		b.align(1, 0);
		Alignment c = Alignment.union(a, b);
		assertTrue(c.aligned(0, 0));
		assertTrue(c.aligned(1, 1));
		assertTrue(c.aligned(1, 0));
		assertTrue(c.aligned(0, 1));
	}
	
	public void testReference() {
		ReferenceAlignment ra = new ReferenceAlignment(4,5);
		ra.align(0, 0);
		ra.sureAlign(1, 1);
		ra.sureAlign(3, 4);
		ra.align(2, 4);
		
		Alignment a = new Alignment(4,5);
		a.align(0, 1);
		a.align(1, 1);
		a.align(2, 4);
		a.align(1, 4);
		assertEquals(1,ra.countSureHits(a));
		
//		Alignment b = new Alignment(3,4);
//		try {
//			ra.countProbableHits(b);
//			fail("Should fail");
//		} catch (RuntimeException f) {}
	}
}
