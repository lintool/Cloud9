package edu.umd.hooka;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;

import junit.framework.TestCase;

public class PhrasePairTest extends TestCase {
	VocabularyWritable ve = new VocabularyWritable();
	VocabularyWritable vf = new VocabularyWritable();
	PhrasePair pp;
	PhrasePair pp2;
	public PhrasePairTest(String name) {
		super(name);
		vf.addOrGet("verde");
		String eline = "a la bruja -ja verde";
		String fline = "NULL ({ 1 }) the ({ 2 }) green ({ 5 }) witch ({ 3 4 })";
		Alignment c = Alignment.fromGiza(eline, fline, true);
		String eline2 = "the green witch";
		String fline2 = "NULL ({ }) a ({ }) la ({ 1 }) bruja ({ 3 }) -ja ({ }) verde ({ 2 })";
		Alignment x = Alignment.fromGiza(eline2, fline2, false);
		Alignment union = Alignment.union(c, x);
		pp = new PhrasePair(eline,vf,eline2,ve,union.toString());
		pp2 = new PhrasePair(eline,vf,eline2,ve,Alignment.intersect(c, x).toString());
	}

	public void testToString() {
		assertEquals("{F:[L=1 2 3 4 5 1] ||| E:[L=0 1 2 3] ||| A: 1-0 2-2 3-2 4-1}", pp.toString());
	}
	
	public void testExtractAlmostMinimalBoundedPhrasePairContainingE() {
		PhrasePair x = pp2.extractMinimalConsistentPhrasePairContainingESpan(0,0);
		assertEquals("la ||| the ||| 0-0", x.toString(vf,ve));
		x = pp2.extractMinimalConsistentPhrasePairContainingESpan(1,2);
		assertEquals("bruja -ja verde ||| green witch ||| 0-1 2-0", x.toString(vf,ve));
	}

	public void testToStringVocabVocab() {
		assertEquals("a la bruja -ja verde ||| the green witch ||| 1-0 2-2 3-2 4-1",
				pp.toString(vf,ve));
	}
	
	public void testMergeEnglishWords() {
		PhrasePair x = new PhrasePair("x x x x", vf, "al- kitab al- jadyd", ve, "0-0 1-1 2-2 3-3");
		x.mergeEnglishWords(2, 3, ve.addOrGet("al-jadyd"));
		String res = x.toString(vf,ve);
		assertEquals("x x x x ||| al- kitab al-jadyd ||| 0-0 1-1 2-2 3-2", res);
		System.err.println(res);
		x = new PhrasePair("x x x x y", vf, "reiste am fruehen morgen ab-", ve, ""); x.setAlignment(null);
		x.mergeEnglishWords(0, 4, ve.addOrGet("abreiste"));
		res = x.toString(vf,ve);
		assertEquals("x x x x y ||| abreiste am fruehen morgen", res);
		x = new PhrasePair("x x x x y", vf, "reiste am fruehen morgen ab-", ve, ""); x.setAlignment(null);
		x.mergeEnglishWords(4, 0, ve.addOrGet("abreiste"));
		res = x.toString(vf,ve);
		assertEquals("x x x x y ||| am fruehen morgen abreiste", res);
	}

	public void testGetE() {
		assertEquals(3, pp.getE().size());
	}

	public void testGetF() {
		assertEquals(5, pp.getF().size());
	}
	
	public void testSetE() {
		PhrasePair pt = (PhrasePair)pp.clone();
		assertTrue(pt.equals(pp));
		Phrase e = pt.getE();
		Phrase empty = new Phrase();
		pt.setAlignment(null);
		assertTrue(pt.compareTo(pp) == 0);
		pt.setE(empty);
		assertTrue(pt.compareTo(pp) != 0);
		assertEquals(pt.compareTo(pp), -pp.compareTo(pt));
		pt.setF(e);
		assertTrue(pt.compareTo(pp) != 0);
	}

	public void testHasAlignment() {
		assertTrue(pp.hasAlignment());
	}
	
	public void testEquals() {
		assertFalse(pp.equals(pp2));
	}
	
	public void testHashCode() {
		assertEquals(pp.hashCode(), pp2.hashCode());
	}

	public void testWrite() {
		try {
		File temp = File.createTempFile("phrpr", null);
		temp.deleteOnExit();
		DataOutputStream dos = new DataOutputStream(
			new FileOutputStream(temp));
		pp.write(dos);
		pp2.write(dos);
		PhrasePair px = new PhrasePair(pp.getF(), pp.getE());
		px.write(dos);
		System.out.println(pp.toString(vf,ve));
		System.out.println(pp2.toString(vf,ve));
		System.out.println(px.toString(vf,ve));
		dos.close();
		System.err.println("Size of PPs on disk: " + dos.size());
		DataInputStream dis = new DataInputStream(
				new FileInputStream(temp));
		PhrasePair pl = new PhrasePair();
		pl.readFields(dis);
		
		assertEquals(pp.toString(vf,ve), pl.toString(vf,ve));
		assertTrue(pl.equals(pp));
		} catch (IOException e) {
			e.printStackTrace();
			fail("Caught "+e);
		}
	}

	public void testExtractConsistentPhrasePairs() {
		java.util.ArrayList<PhrasePair> ps = pp.extractConsistentPhrasePairs(6);
		ArrayList<PhrasePair.SubPhraseCoordinates> pc = pp.extractConsistentSubPhraseCoordinates(6);
		assertEquals(ps.size(), 7);
		assertEquals(ps.size(), 7);
		for (int i=0; i<7; i++) {
			PhrasePair a = ps.get(i);
			PhrasePair b = pp.extractSubPhrasePair(pc.get(i));
			assertTrue(a.equals(b));
		}
		ps = pp.extractConsistentPhrasePairs(2);
		assertEquals(ps.size(), 4);
		assertEquals(pp.extractConsistentSubPhraseCoordinates(2).size(), 4);
		
		assertTrue(ps.get(0).hasAlignment());
	}
}
