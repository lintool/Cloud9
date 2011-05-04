package edu.umd.hooka;

import junit.framework.TestCase;

public class PhraseTest extends TestCase {

	Phrase p1;
	Phrase p2;
	Phrase ps;
	Phrase px;
	VocabularyWritable v1 = new VocabularyWritable();
	VocabularyWritable v2 = new VocabularyWritable();
	
	public PhraseTest(String name) {
		super(name);
		p1 = Phrase.fromString(0, "a b c", v1);
		p2 = Phrase.fromString(0, "d e f", v1);
		ps = Phrase.fromString(0, "a b", v1);
		px = Phrase.fromString(1, "d e f", v2);
	}
	
	public void testVocab() {
		assertEquals(7, v1.size());
		assertEquals("NULL", v1.get(0));
		assertEquals("f", v1.get(6));
	}

	public void testHashCode() {
		assertFalse(p1.hashCode() == p2.hashCode());
		assertFalse(p2.hashCode() == px.hashCode());
		assertFalse(p1.hashCode() == px.hashCode());
	}

	public void testCompareTo() {
		assertEquals(p1.compareTo(p2), -p2.compareTo(p1));
		assertEquals(p1.compareTo(px), -px.compareTo(p1));
		assertEquals(p1.compareTo(ps), -ps.compareTo(p1));
		assertTrue(p1.compareTo(p2) != 0);
		assertTrue(p1.compareTo(px) != 0);
		assertTrue(p1.compareTo(ps) != 0);
		assertEquals(p1.compareTo(p1), 0);
	}

	public void testFromString() {
		assertEquals("foo bar", Phrase.fromString(0, "foo bar", v1).toString(v1));
	}

}
