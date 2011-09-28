package edu.umd.hooka;

import static org.junit.Assert.*;

import org.junit.Test;

import junit.framework.TestCase;

public class Phrase2CountMapTest extends TestCase {

	@Test
	public void testPlusEquals() {
		int[] e1 = {3, 15, 2, 1};
		int[] e2 = {3, 18, 2, 1};
		int[] e3 = {4, 15, 2, 1};
		int[] e4 = {3, 15};
		int[] e5 = {10};
		Phrase p1 = new Phrase(e1, 0);
		Phrase p2 = new Phrase(e2, 0);
		Phrase p3 = new Phrase(e3, 0);
		Phrase p4 = new Phrase(e4, 0);
		Phrase p5 = new Phrase(e5, 0);
		Phrase2CountMap pcm1 = new Phrase2CountMap();
		pcm1.setPhraseCount(p1, 1.0f);
		pcm1.setPhraseCount(p2, 1.0f);
		pcm1.setPhraseCount(p3, 1.0f);
		pcm1.setPhraseCount(p4, 1.0f);
		Phrase2CountMap pcm2 = new Phrase2CountMap();
		pcm2.setPhraseCount(p1, 1.0f);
		pcm2.setPhraseCount(p4, 5.0f);
		pcm2.setPhraseCount(p5, 3.0f);
		
		pcm1.plusEquals(pcm2);
		float d = 0.00001f;
		assertEquals(6.0f, pcm1.getPhraseCount(p4), d);
		assertEquals(2.0f, pcm1.getPhraseCount(p1), d);
		assertEquals(3.0f, pcm1.getPhraseCount(p5), d);
		assertEquals(0.0f, pcm2.getPhraseCount(p2), d);
		
		pcm1.normalize();
		assertEquals(0.461538f, pcm1.getPhraseCount(p4), d);
	}

}
