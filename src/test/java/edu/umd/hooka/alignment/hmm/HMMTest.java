package edu.umd.hooka.alignment.hmm;

import junit.framework.TestCase;
import edu.umd.hooka.Alignment;
import edu.umd.hooka.AlignmentPosteriorGrid;
import edu.umd.hooka.Phrase;
import edu.umd.hooka.PhrasePair;
import edu.umd.hooka.alignment.PerplexityReporter;
import edu.umd.hooka.ttables.TTable_monolithic;

public class HMMTest extends TestCase {

	HMM hmm;
	public void testHMM() {
		ATable at = new ATable(true, 1, 5);
		int[] e1 = {-1,0,3,6};
		int[]ef = {1,2,3,1,2,3};
		TTable_monolithic tt = new TTable_monolithic(e1, ef, 4);
		tt.set(1, 1, 0.1f);
		tt.set(1, 2, 0.7f);
		tt.set(1, 3, 0.2f);
		tt.set(2, 1, 0.5f);
		tt.set(2, 2, 0.4f);
		tt.set(2, 3, 0.1f);
		/*
		at.add(1, 999, 0.8f);
		at.add(0, 999, 0.2f);
		at.add(-1, 999, 0.4f);
		at.add(-2, 999, 0.2f);*/
		at.normalize();
		// System.out.println(at);
		int[] fw = {2, 1, 2, 3};
		//int[] fw = {2, 1, 2};
		int[] ew = {1, 2, 1}; 
		Phrase f = new Phrase(fw, 1);
		Phrase e = new Phrase(ew, 1);
		PhrasePair pp = new PhrasePair(f, e);
		hmm = new HMM(tt, at);
		hmm.buildHMMTables(pp);
		hmm.baumWelch(pp, null);
		TTable_monolithic tc = (TTable_monolithic)tt.clone(); tc.clear();
		ATable ac = (ATable)at.clone(); ac.clear();
		hmm.addPartialTranslationCountsToTTable(tc);
		hmm.addPartialJumpCountsToATable(ac);
		System.out.println("COUNTS:\n" + tc);
		tc.normalize();
		ac.normalize();
		System.out.println("OLD:\n" + at);
		System.out.println("NEW:\n" + ac);
		System.out.println("\nnew:\n"+tc);
		//if (true) return;
		PerplexityReporter cr = new PerplexityReporter();
		Alignment a = hmm.viterbiAlign(pp, cr);
		double ce1 = cr.getCrossEntropy();
		System.out.println(a.toStringVisual() + "\n"+cr);
		assertTrue(a.aligned(0, 0));
		System.out.println(hmm.backtrace);
		hmm = new HMM(tc, ac);
		hmm.buildHMMTables(pp);
		cr.reset();
		a = hmm.viterbiAlign(pp, cr);
		double ce2 = cr.getCrossEntropy();
		// perplexity should decrease!
		assertTrue(ce1 > ce2);
		assert(tc.get(1,1) > 0.0f);
		System.out.println(a.toStringVisual() + "\n" + cr + "\nPG::\n" + hmm.computeAlignmentPosteriors(pp));
		assertEquals(a.getELength(), e.size());
		assertEquals(a.getFLength(), f.size());
	}

	public void testHMM2() {
		ATable at = new ATable(true, 1, 5);
		int[] e1 = {-1,0,2,4};
		int[]ef = {1,2,1,2};
		TTable_monolithic tt = new TTable_monolithic(e1, ef, 4);
		tt.set(1, 1, 0.3f);
		tt.set(1, 2, 0.7f);
		tt.set(2, 1, 0.9f);
		tt.set(2, 2, 0.1f);
		at.add(1, 'a', 999, 0.3f);
		at.add(0, 'a', 999, 0.5f);
		at.add(-1, 'a', 999, 0.4f);
		at.add(-2, 'a', 999, 0.2f);
		at.normalize();
		// System.out.println(at);
		int[] fw = {1, 2};
		//int[] fw = {2, 1, 2};
		int[] ew = {2, 1}; 
		Phrase f = new Phrase(fw, 1);
		Phrase e = new Phrase(ew, 1);
		PhrasePair pp = new PhrasePair(f, e);
		hmm = new HMM(tt, at);
		hmm.buildHMMTables(pp);
		PerplexityReporter cr = new PerplexityReporter();
		Alignment a = hmm.viterbiAlign(pp, cr);
		assertEquals(a.getELength(), e.size());
		assertEquals(a.getFLength(), f.size());
		System.out.println(a.toStringVisual() + "\n"+cr);
	}
	
	public void testUnalignable() {
		ATable at = new ATable(true, 1, 10);
		int[] e1 = {-1,0,2,4};
		int[]ef = {1,2,1,2};
		TTable_monolithic tt = new TTable_monolithic(e1, ef, 4);
		tt.set(1, 1, 0.2f);
		tt.set(1, 2, 0.8f);
		tt.set(2, 1, 0.9f);
		tt.set(2, 2, 0.1f);
		at.add(4, 'a', 999, 0.05f);
		at.add(3, 'a', 999, 0.05f);
		at.add(2, 'a', 999, 0.05f);
		at.add(1, 'a', 999, 0.3f);
		at.add(0, 'a', 999, 0.5f);
		at.add(-1, 'a', 999, 0.4f);
		at.add(-2, 'a', 999, 0.5f);
		at.add(-3, 'a', 999, 0.05f);
		at.add(-4, 'a', 999, 0.05f);
		at.add(-5, 'a', 999, 0.05f);
		at.normalize();
		// System.out.println(at);
		int[] fw = {2, 1, 2, 3, 2, 2, 2};
		//int[] fw = {2, 1, 2};
		int[] ew = {2, 1, 1, 1, 2}; 
		Phrase f = new Phrase(fw, 1);
		Phrase e = new Phrase(ew, 1);
		PhrasePair pp = new PhrasePair(f, e);
		tt.normalize();
		hmm = new HMM(tt, at);
		hmm.buildHMMTables(pp);
		PerplexityReporter cr = new PerplexityReporter();
		Alignment a = hmm.viterbiAlign(pp, cr);
		assertEquals(a.getELength(), e.size());
		assertEquals(a.getFLength(), f.size());
		System.err.println(hmm.emission);
		System.err.println(hmm.transition);
		System.out.println(tt);
		System.err.println(hmm.viterbi);
		System.err.println(hmm.backtrace);
	}

	public void testNullHMM() {
		ATable at = new ATable(true, 1, 5);
		int[] e1 = {-1,0,3,6};
		int[]ef = {1,2,3,1,2,3};
		TTable_monolithic tt = new TTable_monolithic(e1, ef, 4);
		tt.set(0, 1, 0.1f);
		tt.set(0, 2, 0.1f);
		tt.set(0, 3, 0.8f);
		tt.set(1, 1, 0.3f);
		tt.set(1, 2, 0.7f);
		tt.set(2, 1, 0.9f);
		tt.set(2, 2, 0.1f);
		at.add(1, 'a', 999, 0.3f);
		at.add(0, 'a', 999, 0.5f);
		at.add(-1, 'a', 999, 0.4f);
		at.add(-2, 'a', 999, 0.2f);
		at.normalize();
		// System.out.println(at);
		int[] fw = {1, 3, 2, 1};
		//int[] fw = {2, 1, 2};
		int[] ew = {2, 1, 2}; 
		Phrase f = new Phrase(fw, 1);
		Phrase e = new Phrase(ew, 1);
		PhrasePair pp = new PhrasePair(f, e);
		hmm = new HMM_NullWord(tt, at, -1.0);
		hmm.buildHMMTables(pp);
		hmm.baumWelch(pp, null);
		TTable_monolithic tc = (TTable_monolithic)tt.clone(); tc.clear();
		ATable ac = (ATable)at.clone(); ac.clear();
		hmm.addPartialTranslationCountsToTTable(tc);
		hmm.addPartialJumpCountsToATable(ac);
		System.out.println("COUNTS:\n" + tc);
		tc.normalize();
		ac.normalize();
		
		PerplexityReporter cr = new PerplexityReporter();
		Alignment a = hmm.viterbiAlign(pp, cr);
		assertEquals(a.getELength(), e.size());
		assertEquals(a.getFLength(), f.size());
		AlignmentPosteriorGrid pg = hmm.computeAlignmentPosteriors(pp);
		System.out.println(a.toStringVisual() + "\nPG:\n"+pg + "\n"+cr + "\n");
		System.out.println(hmm.emission);
		System.out.println(hmm.transition);
		System.out.println("Done NULL");
	}

}
